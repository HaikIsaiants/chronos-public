param(
    [int]$BasePort = 28101,
    [int]$Workflows = 40,
    [int]$SnapshotEntries = 8,
    [string]$Binaries = ""
)

$ErrorActionPreference = "Stop"
if ($BasePort -lt 1024 -or $BasePort -gt 64000 -or $Workflows -lt 1 -or $SnapshotEntries -lt 1) {
    throw "invalid process test configuration"
}

$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$run = [IO.Path]::GetFullPath((Join-Path $tempRoot ("chronos e2e " + [guid]::NewGuid().ToString("N"))))
if (-not $run.StartsWith($tempRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw "invalid process test path"
}
[IO.Directory]::CreateDirectory($run) | Out-Null

$coordinatorBinary = Join-Path $run "chronos-coordinator.exe"
$workerBinary = Join-Path $run "chronos-worker.exe"
$loadBinary = Join-Path $run "chronos-load.exe"
$script:children = @()
$script:coordinators = @{}
$http = @{}
$workers = @{}
for ($id = 1; $id -le 3; $id++) {
    $http[$id] = "http://127.0.0.1:$($BasePort + $id - 1)"
    $workers[$id] = "127.0.0.1:$($BasePort + 100 + $id - 1)"
}
$peers = "1=$($http[1]),2=$($http[2]),3=$($http[3])"
$workerEndpoints = "$($workers[1]),$($workers[2]),$($workers[3])"
$ackFaultFile = Join-Path $run "completion-ack-fault"

function Start-ChronosProcess {
    param([string]$File, [string[]]$Arguments, [string]$Name)
    $argumentLine = ($Arguments | ForEach-Object { '"' + $_ + '"' }) -join " "
    $process = Start-Process -FilePath $File -ArgumentList $argumentLine -PassThru -WindowStyle Hidden -RedirectStandardOutput (Join-Path $run "$Name.out.log") -RedirectStandardError (Join-Path $run "$Name.err.log")
    $script:children += $process
    return $process
}

function Start-Coordinator {
    param([int]$Id)
    $arguments = @(
        "-id", "$Id",
        "-cluster", "process-e2e",
        "-data", (Join-Path $run "node-$Id"),
        "-listen", $http[$Id].Substring(7),
        "-worker-listen", $workers[$Id],
        "-peers", $peers,
        "-lease", "2s",
        "-max-pending", "256",
        "-snapshot-entries", "$SnapshotEntries",
        "-completion-ack-fault-file", $ackFaultFile
    )
    $process = Start-ChronosProcess $coordinatorBinary $arguments "coordinator-$Id-$([guid]::NewGuid().ToString('N'))"
    $script:coordinators[$Id] = $process
}

function Wait-Healthy {
    param([int]$Id)
    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    while ([DateTime]::UtcNow -lt $deadline) {
        try {
            $health = Invoke-RestMethod -Uri "$($http[$Id])/healthz" -TimeoutSec 1
            if ($health.status -eq "ok" -and $health.node_id -eq $Id) {
                return
            }
        } catch {
        }
        Start-Sleep -Milliseconds 50
    }
    throw "coordinator $Id did not become healthy"
}

function Find-Leader {
    param([int]$Excluded = 0)
    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    while ([DateTime]::UtcNow -lt $deadline) {
        for ($id = 1; $id -le 3; $id++) {
            if ($id -eq $Excluded) {
                continue
            }
            try {
                $status = Invoke-RestMethod -Uri "$($http[$id])/v1/status" -TimeoutSec 1
                if ($status.role -eq "StateLeader" -and $status.leader_id -eq $id) {
                    return $id
                }
            } catch {
            }
        }
        Start-Sleep -Milliseconds 50
    }
    throw "leader was not elected"
}

function Run-Load {
    param([string]$Prefix, [int]$Coordinator)
    & $loadBinary -coordinator $http[$Coordinator] -namespaces 3 -workflows $Workflows -concurrency 8 -task-type work -prefix $Prefix -wait -timeout 1m
    if ($LASTEXITCODE -ne 0) {
        throw "load $Prefix failed"
    }
}

function Submit-Workflow {
    param([hashtable]$Definition, [string]$RequestId, [int]$Coordinator)
    $now = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $command = @{
        kind = "submit_workflow"
        request_id = $RequestId
        at = $now
        definition = $Definition
    } | ConvertTo-Json -Depth 20 -Compress
    $response = Invoke-WorkflowCommand -Command $command -Coordinator $Coordinator
    Assert-CommandProof -Response $response -RequestId $RequestId
    return $response.results[0].workflow_id
}

function Invoke-WorkflowCommand {
    param([string]$Command, [int]$Coordinator)
    return (Invoke-RestMethod -Method Post -Uri "$($http[$Coordinator])/v1/commands" -ContentType "application/json" -Body $Command -TimeoutSec 10)
}

function Assert-CommandProof {
    param($Response, [string]$RequestId)
    $results = @($Response.results)
    $commits = @($Response.commits)
    if ($results.Count -ne 1 -or $commits.Count -ne 1 -or $commits[0].request_id -ne $RequestId -or [uint64]$commits[0].applied_index -eq 0 -or [uint64]$commits[0].committed_term -eq 0 -or [uint64]$Response.applied_index -ne [uint64]$commits[0].applied_index -or [uint64]$Response.committed_term -ne [uint64]$commits[0].committed_term) {
        throw "command commit proof is invalid"
    }
}

function Submit-SnapshotReceipt {
    param([int]$Coordinator)
    $retry = @{max_attempts = 1; initial_backoff_millis = 0; backoff_multiplier = 1; max_backoff_millis = 0}
    $definition = @{
        name = "snapshot-receipt"
        namespace = "process"
        tasks = @(
            @{
                id = "hold"
                type = "snapshot-hold"
                dependencies = @()
                retry = $retry
                timeout_millis = 0
            }
        )
    }
    $requestId = "snapshot-receipt"
    $command = @{
        kind = "submit_workflow"
        request_id = $requestId
        at = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
        definition = $definition
    } | ConvertTo-Json -Depth 20 -Compress
    $response = Invoke-WorkflowCommand -Command $command -Coordinator $Coordinator
    Assert-CommandProof -Response $response -RequestId $requestId
    $events = ConvertTo-Json -InputObject @($response.results[0].events) -Depth 20 -Compress
    return [pscustomobject]@{
        body = $command
        request_id = $requestId
        workflow_id = [string]$response.results[0].workflow_id
        applied_index = [uint64]$response.commits[0].applied_index
        committed_term = [uint64]$response.commits[0].committed_term
        events = $events
    }
}

function Assert-DurableDedupe {
    param($Receipt, [int]$Coordinator)
    $response = Invoke-WorkflowCommand -Command $Receipt.body -Coordinator $Coordinator
    Assert-CommandProof -Response $response -RequestId $Receipt.request_id
    $events = ConvertTo-Json -InputObject @($response.results[0].events) -Depth 20 -Compress
    if (-not $response.results[0].duplicate -or [string]$response.results[0].workflow_id -ne $Receipt.workflow_id -or [uint64]$response.commits[0].applied_index -ne $Receipt.applied_index -or [uint64]$response.commits[0].committed_term -ne $Receipt.committed_term -or $events -cne $Receipt.events) {
        throw "snapshot changed durable command receipt"
    }
}

function Get-SnapshotCount {
    param([int]$Coordinator)
    $response = Invoke-WebRequest -Uri "$($http[$Coordinator])/metrics" -UseBasicParsing -TimeoutSec 2
    $match = [regex]::Match([string]$response.Content, '(?m)^chronos_snapshots_total\{kind="create",result="success"\}\s+([0-9eE+.-]+)[ \t\r]*$')
    if (-not $match.Success) {
        return 0
    }
    return [double]$match.Groups[1].Value
}

function Get-SnapshotCounts {
    $counts = @{}
    for ($id = 1; $id -le 3; $id++) {
        $counts[$id] = Get-SnapshotCount -Coordinator $id
    }
    return $counts
}

function Wait-AutomaticSnapshots {
    param([hashtable]$Baseline, [uint64]$MinimumApplied)
    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    while ([DateTime]::UtcNow -lt $deadline) {
        $counts = @{}
        $ready = $true
        for ($id = 1; $id -le 3; $id++) {
            try {
                $status = Invoke-RestMethod -Uri "$($http[$id])/v1/status" -TimeoutSec 1
                $counts[$id] = Get-SnapshotCount -Coordinator $id
                if ([uint64]$status.applied_index -lt $MinimumApplied -or [double]$counts[$id] -le [double]$Baseline[$id]) {
                    $ready = $false
                }
            } catch {
                $ready = $false
                break
            }
        }
        if ($ready) {
            return $counts
        }
        Start-Sleep -Milliseconds 50
    }
    throw "automatic snapshot compaction did not complete"
}

function Submit-LifecycleWorkflow {
    param([int]$Coordinator)
    $retry = @{max_attempts = 1; initial_backoff_millis = 0; backoff_multiplier = 1; max_backoff_millis = 0}
    $childRetry = @{max_attempts = 2; initial_backoff_millis = 25; backoff_multiplier = 2; max_backoff_millis = 100}
    $verifyRetry = @{max_attempts = 2; initial_backoff_millis = 50; backoff_multiplier = 2; max_backoff_millis = 100}
    $startAt = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() + 10000
    $definition = @{
        name = "lifecycle-e2e"
        namespace = "e2e"
        start_at = $startAt
        tasks = @(
            @{
                id = "discover"
                type = "fanout"
                dependencies = @()
                retry = $retry
                timeout_millis = 5000
                fanout = @{
                    task_type = "work"
                    aggregate_task_id = "aggregate"
                    max_items = 2
                    retry = $childRetry
                    timeout_millis = 5000
                }
            },
            @{
                id = "aggregate"
                type = "aggregate"
                dependencies = @("discover")
                retry = $retry
                timeout_millis = 5000
            },
            @{
                id = "deploy"
                type = "deploy"
                dependencies = @("aggregate")
                retry = $retry
                timeout_millis = 5000
                compensation = @{task_type = "rollback"}
            },
            @{
                id = "verify"
                type = "verify"
                dependencies = @("publish")
                retry = $verifyRetry
                timeout_millis = 5000
            },
            @{
                id = "publish"
                type = "publish"
                dependencies = @("deploy")
                retry = $retry
                timeout_millis = 5000
                compensation = @{task_type = "rollback"}
            }
        )
    }
    $workflowId = Submit-Workflow -Definition $definition -RequestId "lifecycle-e2e" -Coordinator $Coordinator
    return [pscustomobject]@{workflow_id = $workflowId; start_at = $startAt}
}

function Read-Workflow {
    param([string]$WorkflowId, [int]$Coordinator)
    return (Invoke-RestMethod -Uri "$($http[$Coordinator])/v1/workflows/$WorkflowId" -TimeoutSec 2)
}

function Wait-Workflow {
    param([string]$WorkflowId, [string]$Expected, [int]$Coordinator)
    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    $lastStatus = "unavailable"
    $lastError = ""
    while ([DateTime]::UtcNow -lt $deadline) {
        try {
            $workflow = Read-Workflow -WorkflowId $WorkflowId -Coordinator $Coordinator
        } catch {
            $lastError = $_.Exception.Message
            Start-Sleep -Milliseconds 50
            continue
        }
        $lastStatus = [string]$workflow.state.status
        if ($lastStatus -eq $Expected) {
            return $workflow
        }
        if (@("completed", "failed", "cancelled", "compensated") -contains $lastStatus) {
            throw "workflow $WorkflowId reached $lastStatus instead of $Expected"
        }
        Start-Sleep -Milliseconds 50
    }
    throw "workflow $WorkflowId did not reach $Expected; last_status=$lastStatus last_error=$lastError"
}

function Wait-Task {
    param([string]$WorkflowId, [string]$TaskId, [string]$Expected, [int]$Coordinator)
    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    while ([DateTime]::UtcNow -lt $deadline) {
        try {
            $workflow = Read-Workflow -WorkflowId $WorkflowId -Coordinator $Coordinator
            $task = $workflow.state.tasks.PSObject.Properties[$TaskId].Value
            if ($task.status -eq $Expected) {
                return $workflow
            }
            if (@("completed", "failed", "cancelled", "compensated") -contains [string]$workflow.state.status) {
                throw "workflow $WorkflowId reached $($workflow.state.status) before $TaskId reached $Expected"
            }
        } catch {
            if ($_.Exception.Message -like "workflow $WorkflowId reached*") {
                throw
            }
        }
        Start-Sleep -Milliseconds 50
    }
    throw "task $WorkflowId/$TaskId did not reach $Expected"
}

function Wait-AckFaultState {
    param([string]$Expected)
    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    while ([DateTime]::UtcNow -lt $deadline) {
        if ([IO.File]::Exists($ackFaultFile)) {
            $state = [IO.File]::ReadAllText($ackFaultFile).Trim()
            if ($state -eq $Expected) {
                return
            }
        }
        Start-Sleep -Milliseconds 50
    }
    throw "completion acknowledgement fault did not reach $Expected"
}

function Assert-ScheduledWorkflow {
    param($Workflow, [int64]$StartAt)
    $timers = @($Workflow.state.timers.PSObject.Properties | ForEach-Object { $_.Value })
    if ($Workflow.state.status -ne "scheduled" -or $timers.Count -ne 1 -or $timers[0].purpose -ne "workflow_start" -or $timers[0].status -ne "scheduled" -or [int64]$timers[0].deadline -ne $StartAt) {
        throw "scheduled workflow state is invalid"
    }
}

function Assert-LifecycleWorkflow {
    param($Workflow)
    $state = $Workflow.state
    $discover = $state.tasks.discover
    $children = @($discover.children)
    if ($state.status -ne "compensated" -or $discover.status -ne "completed" -or $children.Count -ne 2) {
        throw "fanout state is invalid"
    }
    $values = @()
    foreach ($childId in $children) {
        $child = $state.tasks.PSObject.Properties[$childId].Value
        if ($null -eq $child -or $child.status -ne "completed" -or $child.definition.type -ne "work") {
            throw "fanout child is invalid"
        }
        $values += [string]$child.payload.value
        if (@($state.tasks.aggregate.inputs.PSObject.Properties.Name) -notcontains $childId) {
            throw "aggregate is missing child $childId"
        }
    }
    if (($values | Sort-Object) -join "," -ne "item-1,item-2" -or @($state.tasks.aggregate.inputs.PSObject.Properties).Count -ne 3 -or $state.tasks.aggregate.status -ne "completed") {
        throw "aggregate state is invalid"
    }
    $deploy = $state.tasks.deploy
    $publish = $state.tasks.publish
    $verify = $state.tasks.verify
    if ($deploy.status -ne "compensated" -or $publish.status -ne "compensated" -or $verify.status -ne "failed" -or [uint32]$verify.attempt -ne 2 -or [uint32]$verify.retry_count -ne 1) {
        throw "retry or compensation state is invalid"
    }
    if ([uint64]$deploy.completed_sequence -ge [uint64]$publish.completed_sequence -or [int64]$publish.finished_at -ge [int64]$deploy.finished_at) {
        throw "compensation order is invalid"
    }
    $scheduledTimers = @($state.timers.PSObject.Properties | Where-Object { $_.Value.status -eq "scheduled" })
    if ($scheduledTimers.Count -ne 0) {
        throw "workflow left scheduled timers"
    }
}

function Run-Cancellation {
    param([int]$Coordinator)
    $retry = @{max_attempts = 1; initial_backoff_millis = 0; backoff_multiplier = 1; max_backoff_millis = 0}
    $definition = @{
        name = "cancellation-e2e"
        namespace = "e2e"
        tasks = @(
            @{
                id = "hold"
                type = "hold"
                dependencies = @()
                retry = $retry
                timeout_millis = 0
            }
        )
    }
    $workflowId = Submit-Workflow -Definition $definition -RequestId "cancellation-e2e" -Coordinator $Coordinator
    Wait-Task -WorkflowId $workflowId -TaskId "hold" -Expected "running" -Coordinator $Coordinator | Out-Null
    $body = @{request_id = "cancellation-e2e-request"; reason = "process test"} | ConvertTo-Json -Compress
    Invoke-RestMethod -Method Post -Uri "$($http[$Coordinator])/v1/workflows/$workflowId/cancel" -ContentType "application/json" -Body $body -TimeoutSec 10 | Out-Null
    $workflow = Wait-Workflow -WorkflowId $workflowId -Expected "cancelled" -Coordinator $Coordinator
    if ($workflow.state.tasks.hold.status -ne "cancelled" -or [uint32]$workflow.state.tasks.hold.attempt -ne 1) {
        throw "running cancellation state is invalid"
    }
    return [pscustomobject]@{workflow_id = $workflowId; version = [uint64]$workflow.state.version}
}

function Run-Timeout {
    param([int]$Coordinator)
    $retry = @{max_attempts = 2; initial_backoff_millis = 50; backoff_multiplier = 2; max_backoff_millis = 100}
    $definition = @{
        name = "timeout-e2e"
        namespace = "e2e"
        tasks = @(
            @{
                id = "slow"
                type = "slow"
                dependencies = @()
                retry = $retry
                timeout_millis = 25
            }
        )
    }
    $workflowId = Submit-Workflow -Definition $definition -RequestId "timeout-e2e" -Coordinator $Coordinator
    $workflow = Wait-Workflow -WorkflowId $workflowId -Expected "failed" -Coordinator $Coordinator
    $task = $workflow.state.tasks.slow
    $timers = @($workflow.state.timers.PSObject.Properties | ForEach-Object { $_.Value })
    $timeouts = @($timers | Where-Object { $_.purpose -eq "timeout" -and $_.status -eq "fired" })
    $retries = @($timers | Where-Object { $_.purpose -eq "retry" -and $_.status -eq "fired" })
    if ($task.status -ne "failed" -or $task.error -ne "task timed out" -or [uint32]$task.attempt -ne 2 -or [uint32]$task.retry_count -ne 1 -or $timeouts.Count -ne 2 -or $retries.Count -ne 1) {
        throw "timeout retry state is invalid"
    }
    return [pscustomobject]@{workflow_id = $workflowId; version = [uint64]$workflow.state.version}
}

function Wait-Converged {
    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    while ([DateTime]::UtcNow -lt $deadline) {
        $indexes = @()
        for ($id = 1; $id -le 3; $id++) {
            try {
                $status = Invoke-RestMethod -Uri "$($http[$id])/v1/status" -TimeoutSec 1
                $indexes += [uint64]$status.applied_index
            } catch {
                $indexes = @()
                break
            }
        }
        if ($indexes.Count -eq 3 -and $indexes[0] -gt 1 -and ($indexes | Select-Object -Unique).Count -eq 1) {
            return $indexes[0]
        }
        Start-Sleep -Milliseconds 50
    }
    throw "coordinators did not converge"
}

try {
    if ($Binaries) {
        $source = [IO.Path]::GetFullPath($Binaries)
        Copy-Item -LiteralPath (Join-Path $source "chronos-coordinator.exe") -Destination $coordinatorBinary
        Copy-Item -LiteralPath (Join-Path $source "chronos-worker.exe") -Destination $workerBinary
        Copy-Item -LiteralPath (Join-Path $source "chronos-load.exe") -Destination $loadBinary
    } else {
        Push-Location $root
        try {
            & go build -o $coordinatorBinary ./cmd/chronos-coordinator
            if ($LASTEXITCODE -ne 0) { throw "coordinator build failed" }
            & go build -o $workerBinary ./cmd/chronos-worker
            if ($LASTEXITCODE -ne 0) { throw "worker build failed" }
            & go build -o $loadBinary ./cmd/chronos-load
            if ($LASTEXITCODE -ne 0) { throw "load build failed" }
        } finally {
            Pop-Location
        }
    }
    for ($id = 1; $id -le 3; $id++) {
        Start-Coordinator $id
    }
    for ($id = 1; $id -le 3; $id++) {
        Wait-Healthy $id
    }
    $leader = Find-Leader
    $capabilities = "work,fanout,aggregate,rollback"
    Start-ChronosProcess $workerBinary @("-coordinators", $workerEndpoints, "-id", "worker-1", "-capabilities", $capabilities, "-fanout-types", "fanout", "-delay-types", "rollback", "-delay", "100ms", "-credits", "4") "worker-1" | Out-Null
    Start-ChronosProcess $workerBinary @("-coordinators", $workerEndpoints, "-id", "worker-2", "-capabilities", $capabilities, "-fanout-types", "fanout", "-delay-types", "rollback", "-delay", "100ms", "-credits", "4") "worker-2" | Out-Null
    Start-ChronosProcess $workerBinary @("-coordinators", $workerEndpoints, "-id", "worker-3", "-capabilities", "slow,hold", "-delay-types", "slow,hold", "-delay", "2s", "-credits", "1") "worker-3" | Out-Null
    Start-ChronosProcess $workerBinary @("-coordinators", $workerEndpoints, "-id", "worker-4", "-capabilities", "slow,hold", "-delay-types", "slow,hold", "-delay", "2s", "-credits", "1") "worker-4" | Out-Null
    $deployWorker = Start-ChronosProcess $workerBinary @("-coordinators", $workerEndpoints, "-id", "worker-deploy-lost", "-capabilities", "deploy", "-delay-types", "deploy", "-delay", "10s", "-credits", "1") "worker-deploy-lost"
    $snapshotReceipt = Submit-SnapshotReceipt $leader
    $snapshotBaseline = Get-SnapshotCounts
    Run-Load "before" $leader
    $snapshotCounts = Wait-AutomaticSnapshots -Baseline $snapshotBaseline -MinimumApplied ($snapshotReceipt.applied_index + [uint64]$SnapshotEntries)
    Assert-DurableDedupe -Receipt $snapshotReceipt -Coordinator $leader
    $lifecycle = Submit-LifecycleWorkflow $leader
    $scheduled = Wait-Workflow -WorkflowId $lifecycle.workflow_id -Expected "scheduled" -Coordinator $leader
    Assert-ScheduledWorkflow -Workflow $scheduled -StartAt $lifecycle.start_at
    $failover = [Diagnostics.Stopwatch]::StartNew()
    Stop-Process -Id $script:coordinators[$leader].Id -Force
    $newLeader = Find-Leader $leader
    $failover.Stop()
    Assert-DurableDedupe -Receipt $snapshotReceipt -Coordinator $newLeader
    $scheduled = Wait-Workflow -WorkflowId $lifecycle.workflow_id -Expected "scheduled" -Coordinator $newLeader
    Assert-ScheduledWorkflow -Workflow $scheduled -StartAt $lifecycle.start_at
    $runningDeploy = Wait-Task -WorkflowId $lifecycle.workflow_id -TaskId "deploy" -Expected "running" -Coordinator $newLeader
    $firstDeploy = $runningDeploy.state.tasks.deploy
    if ($firstDeploy.worker_id -ne "worker-deploy-lost" -or [uint32]$firstDeploy.attempt -ne 1) {
        throw "dedicated deploy worker did not own the first attempt"
    }
    $firstDeployAttempt = [string]$firstDeploy.attempt_id
    $firstDeployFence = [uint64]$firstDeploy.fencing_token
    Stop-Process -Id $deployWorker.Id -Force
    $deployWorker.WaitForExit(5000) | Out-Null
    Start-ChronosProcess $workerBinary @("-coordinators", $workerEndpoints, "-id", "worker-deploy-replacement", "-capabilities", "deploy", "-credits", "1") "worker-deploy-replacement" | Out-Null
    $completedDeploy = Wait-Task -WorkflowId $lifecycle.workflow_id -TaskId "deploy" -Expected "completed" -Coordinator $newLeader
    $finalDeploy = $completedDeploy.state.tasks.deploy
    if ([string]$finalDeploy.attempt_id -eq $firstDeployAttempt -or [uint32]$finalDeploy.attempt -ne 2 -or [uint64]$finalDeploy.fencing_token -le $firstDeployFence -or $finalDeploy.output.worker -ne "worker-deploy-replacement") {
        throw "worker death did not produce a fenced deploy reassignment"
    }
    Wait-Task -WorkflowId $lifecycle.workflow_id -TaskId "publish" -Expected "ready" -Coordinator $newLeader | Out-Null
    [IO.File]::WriteAllText($ackFaultFile, "armed")
    Start-ChronosProcess $workerBinary @("-coordinators", $workerEndpoints, "-id", "worker-publish", "-capabilities", "publish", "-credits", "1") "worker-publish" | Out-Null
    $published = Wait-Task -WorkflowId $lifecycle.workflow_id -TaskId "publish" -Expected "completed" -Coordinator $newLeader
    $publishedVersion = [uint64]$published.state.version
    Wait-AckFaultState -Expected "retried"
    $publishedAfterRetry = Read-Workflow -WorkflowId $lifecycle.workflow_id -Coordinator $newLeader
    if ($publishedAfterRetry.state.tasks.publish.status -ne "completed" -or [uint64]$publishedAfterRetry.state.version -ne $publishedVersion) {
        throw "completion acknowledgement retry changed committed workflow state"
    }
    Start-ChronosProcess $workerBinary @("-coordinators", $workerEndpoints, "-id", "worker-verify", "-capabilities", "verify", "-fail-types", "verify", "-credits", "1") "worker-verify" | Out-Null
    $lifecycleWorkflow = Wait-Workflow -WorkflowId $lifecycle.workflow_id -Expected "compensated" -Coordinator $newLeader
    Assert-LifecycleWorkflow $lifecycleWorkflow
    $timedOut = Run-Timeout $newLeader
    $cancelled = Run-Cancellation $newLeader
    Start-Coordinator $leader
    Wait-Healthy $leader
    Run-Load "after" $newLeader
    Start-Sleep -Milliseconds 2250
    $timeoutAfterStaleCompletion = Read-Workflow -WorkflowId $timedOut.workflow_id -Coordinator $newLeader
    $cancelAfterStaleCompletion = Read-Workflow -WorkflowId $cancelled.workflow_id -Coordinator $newLeader
    if ($timeoutAfterStaleCompletion.state.status -ne "failed" -or [uint64]$timeoutAfterStaleCompletion.state.version -ne $timedOut.version -or $cancelAfterStaleCompletion.state.status -ne "cancelled" -or [uint64]$cancelAfterStaleCompletion.state.version -ne $cancelled.version) {
        throw "stale completion changed terminal workflow state"
    }
    $index = Wait-Converged
    $snapshotCreates = ($snapshotCounts.Values | Measure-Object -Minimum).Minimum
    Write-Output "process_e2e=passed workflows=$($Workflows * 2) lifecycle=$($lifecycle.workflow_id) timeout=$($timedOut.workflow_id) cancelled=$($cancelled.workflow_id) leader_before=$leader leader_after=$newLeader leader_failover_seconds=$([Math]::Round($failover.Elapsed.TotalSeconds, 3)) killed_worker=worker-deploy-lost replacement_fence=$($finalDeploy.fencing_token) completion_ack=retried snapshot_entries=$SnapshotEntries snapshot_creates=$snapshotCreates snapshot_receipt_index=$($snapshotReceipt.applied_index) applied_index=$index"
} catch {
    Get-ChildItem $run -Filter "*.log" -ErrorAction SilentlyContinue | ForEach-Object {
        Write-Output "$($_.Name)"
        Get-Content $_.FullName -ErrorAction SilentlyContinue
    }
    throw
} finally {
    foreach ($process in $script:children) {
        try {
            if (-not $process.HasExited) {
                Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
                $process.WaitForExit(5000) | Out-Null
            }
        } catch {
        }
    }
    for ($attempt = 0; $attempt -lt 5 -and [IO.Directory]::Exists($run); $attempt++) {
        try {
            [IO.Directory]::Delete($run, $true)
        } catch {
            if ($attempt -eq 4) {
                Write-Warning "could not remove process test directory: $($_.Exception.Message)"
            } else {
                Start-Sleep -Milliseconds 100
            }
        }
    }
}
