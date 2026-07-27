<#
.SYNOPSIS
    Suspends Azure Synapse dedicated SQL pools that have been idle longer than a threshold.

.DESCRIPTION
    Designed to run as a PowerShell runbook in Azure Automation using a managed identity.

    For every Synapse workspace in the subscription it enumerates the dedicated SQL pools
    (Microsoft.Synapse/workspaces/sqlPools). For each pool whose status is 'Online' it
    connects to the pool and evaluates sys.dm_pdw_exec_requests:
      - any request with status 'Running' or 'Suspended' (queued)        -> BUSY,  skipped
      - latest completed request newer than (UTC now - threshold)        -> BUSY,  skipped
      - no request history at all (DMVs are cleared on resume/scale)     -> skipped,
        unless -SuspendWhenNoRequestHistory is specified
      - otherwise                                                        -> IDLE,  suspended

    Pools in any other status (Paused, Pausing, Resuming, Scaling) are skipped, because
    compute can only be changed when a pool is Online. A pool that is already Paused
    (the target state) is reported as 'Already suspended' and left untouched - the
    script is idempotent and never fails on pools that are already suspended.

.PARAMETER SubscriptionId
    Target Azure subscription ID (GUID).

.PARAMETER Action
    Action to perform. Currently only 'Suspend' is supported.

.PARAMETER IdleThresholdMinutes
    Minutes without completed query activity before a pool is considered idle. Default 30.

.PARAMETER UserAssignedIdentityClientId
    Optional. Client ID of a user-assigned managed identity. Omit to use the system-assigned one.

.PARAMETER SuspendWhenNoRequestHistory
    Optional. Suspend pools even when sys.dm_pdw_exec_requests holds no rows (DMV history is
    cleared on resume/scale, so by default these pools are skipped as a safety measure).

.EXAMPLE
    .\Suspend-IdleSynapseSqlPools.ps1 -SubscriptionId '11111111-2222-3333-4444-555555555555' -Action Suspend

.EXAMPLE
    .\Suspend-IdleSynapseSqlPools.ps1 -SubscriptionId '11111111-2222-3333-4444-555555555555' -Action Suspend -WhatIf

.NOTES
    Required modules in the Automation account : Az.Accounts, Az.Synapse, SqlServer
    Required RBAC on workspaces                : Synapse Contributor (least privilege)
    Required SQL permission per pool           : CREATE USER [mi-name] FROM EXTERNAL PROVIDER;
                                                 GRANT VIEW DATABASE STATE TO [mi-name];
    Networking                                 : workspace firewall must allow Azure services,
                                                 or run on a Hybrid Runbook Worker in the VNet.
    References:
      - Suspend-AzSynapseSqlPool:
        https://learn.microsoft.com/powershell/module/az.synapse/suspend-azsynapsesqlpool
      - sys.dm_pdw_exec_requests:
        https://learn.microsoft.com/sql/relational-databases/system-dynamic-management-objects/sys-dm-pdw-exec-requests-transact-sql
      - Monitor a dedicated SQL pool using DMVs:
        https://learn.microsoft.com/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-manage-monitor
#>

[CmdletBinding(SupportsShouldProcess = $true)]
param (
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$')]
    [string]$SubscriptionId,

    [Parameter(Mandatory = $true)]
    [ValidateSet('Suspend')]
    [string]$Action,

    [Parameter(Mandatory = $false)]
    [ValidateRange(1, 1440)]
    [int]$IdleThresholdMinutes = 30,

    [Parameter(Mandatory = $false)]
    [string]$UserAssignedIdentityClientId,

    [Parameter(Mandatory = $false)]
    [switch]$SuspendWhenNoRequestHistory
)

$ErrorActionPreference = 'Stop'

#region 1. Verify required modules are available in the Automation account
foreach ($moduleName in 'Az.Accounts', 'Az.Synapse', 'SqlServer') {
    if (-not (Get-Module -ListAvailable -Name $moduleName)) {
        throw "Required module '$moduleName' is not imported into this Automation account."
    }
    Import-Module $moduleName -ErrorAction Stop
}
#endregion

#region 2. Sign in with the Automation account's managed identity
$connectParams = @{ Identity = $true }
if ($UserAssignedIdentityClientId) { $connectParams['AccountId'] = $UserAssignedIdentityClientId }

Connect-AzAccount @connectParams | Out-Null
$null = Set-AzContext -Subscription $SubscriptionId
Write-Output "Connected to subscription '$SubscriptionId' via managed identity."
#endregion

#region 3. Helper: Microsoft Entra access token for the SQL endpoint
function Get-SqlAccessToken {
    # Resource URL for Azure SQL / Synapse SQL endpoints
    $tokenResponse = Get-AzAccessToken -ResourceUrl 'https://database.windows.net' -ErrorAction Stop
    if ($tokenResponse.Token -is [securestring]) {
        # Az.Accounts 5.x+ returns the token as a SecureString
        return [System.Net.NetworkCredential]::new('token', $tokenResponse.Token).Password
    }
    return $tokenResponse.Token
}
#endregion

#region 4. Enumerate all Synapse workspaces in the subscription
$workspaces = Get-AzResource -ResourceType 'Microsoft.Synapse/workspaces' -ErrorAction Stop
if (-not $workspaces) {
    Write-Output 'No Synapse workspaces found in this subscription. Nothing to do.'
    return
}
Write-Output "Found $($workspaces.Count) Synapse workspace(s)."
#endregion

$results  = [System.Collections.Generic.List[object]]::new()
$failures = 0

foreach ($workspace in $workspaces) {
    Write-Output "--- Workspace [$($workspace.Name)] (RG: $($workspace.ResourceGroupName)) ---"

    # Fresh token per workspace (tokens are short-lived; enumeration can take a while)
    $sqlAccessToken = Get-SqlAccessToken
    $sqlEndpoint    = "$($workspace.Name).sql.azuresynapse.net"

    $pools = Get-AzSynapseSqlPool -ResourceGroupName $workspace.ResourceGroupName `
                                  -WorkspaceName $workspace.Name -ErrorAction Stop

    foreach ($pool in $pools) {
        # Compute can only be suspended while the pool is Online.
        # An already-Paused pool is the target state: report it cleanly and move on.
        if ($pool.Status -ne 'Online') {
            $decision = if ($pool.Status -eq 'Paused') { 'Already suspended (target state)' }
                        else { "Skipped (status: $($pool.Status))" }
            Write-Output "Pool [$($pool.Name)]: $decision"
            $results.Add([pscustomobject]@{
                Workspace = $workspace.Name; Pool = $pool.Name
                Decision  = $decision; Detail = ''
            })
            continue
        }

        # Idle check: active requests + last completed request time (DMV times are UTC)
        $idleQuery = @"
SELECT ActiveRequests    = (SELECT COUNT(*)       FROM sys.dm_pdw_exec_requests
                            WHERE [status] IN ('Running', 'Suspended')),
       LastRequestEndUtc = (SELECT MAX([end_time]) FROM sys.dm_pdw_exec_requests
                            WHERE [end_time] IS NOT NULL);
"@

        try {
            $stats = Invoke-Sqlcmd -ServerInstance $sqlEndpoint `
                                   -Database $pool.Name `
                                   -AccessToken $sqlAccessToken `
                                   -Query $idleQuery `
                                   -ConnectionTimeout 30 -QueryTimeout 60 `
                                   -OutputSqlErrors $true -ErrorAction Stop
        }
        catch {
            # Never suspend a pool whose activity could not be evaluated
            $failures++
            Write-Warning "Pool [$($pool.Name)]: idle check failed - $($_.Exception.Message). Left untouched."
            $results.Add([pscustomobject]@{
                Workspace = $workspace.Name; Pool = $pool.Name
                Decision  = 'Skipped (check failed)'; Detail = $_.Exception.Message
            })
            continue
        }

        if ($stats.ActiveRequests -gt 0) {
            Write-Output "Pool [$($pool.Name)]: $($stats.ActiveRequests) active/queued request(s) - busy, skipped."
            $results.Add([pscustomobject]@{
                Workspace = $workspace.Name; Pool = $pool.Name
                Decision  = 'Skipped (busy)'; Detail = "$($stats.ActiveRequests) active request(s)"
            })
            continue
        }

        if ($stats.LastRequestEndUtc -is [DBNull]) {
            # DMV history is empty (cleared on resume/scale) - cannot prove idleness
            if (-not $SuspendWhenNoRequestHistory) {
                Write-Output "Pool [$($pool.Name)]: no request history in DMV - skipped (safety default)."
                $results.Add([pscustomobject]@{
                    Workspace = $workspace.Name; Pool = $pool.Name
                    Decision  = 'Skipped (no request history)'; Detail = 'Use -SuspendWhenNoRequestHistory to override'
                })
                continue
            }
        }
        else {
            $idleFor = (Get-Date).ToUniversalTime() - [datetime]$stats.LastRequestEndUtc
            if ($idleFor.TotalMinutes -le $IdleThresholdMinutes) {
                Write-Output ("Pool [{0}]: last request ended {1:N1} min ago - not idle, skipped." -f $pool.Name, $idleFor.TotalMinutes)
                $results.Add([pscustomobject]@{
                    Workspace = $workspace.Name; Pool = $pool.Name
                    Decision  = 'Skipped (recent activity)'
                    Detail    = ("Idle {0:N1} min (threshold {1})" -f $idleFor.TotalMinutes, $IdleThresholdMinutes)
                })
                continue
            }
            Write-Output ("Pool [{0}]: idle for {1:N1} min - suspending." -f $pool.Name, $idleFor.TotalMinutes)
        }

        # Suspend the pool
        $target = "$($workspace.Name)/$($pool.Name)"
        if ($PSCmdlet.ShouldProcess($target, 'Suspend-AzSynapseSqlPool')) {
            try {
                Suspend-AzSynapseSqlPool -ResourceGroupName $workspace.ResourceGroupName `
                                         -WorkspaceName $workspace.Name `
                                         -Name $pool.Name -ErrorAction Stop | Out-Null
                $results.Add([pscustomobject]@{
                    Workspace = $workspace.Name; Pool = $pool.Name
                    Decision  = 'Suspended'; Detail = ''
                })
            }
            catch {
                $failures++
                Write-Warning "Pool [$($pool.Name)]: suspend failed - $($_.Exception.Message)"
                $results.Add([pscustomobject]@{
                    Workspace = $workspace.Name; Pool = $pool.Name
                    Decision  = 'Suspend FAILED'; Detail = $_.Exception.Message
                })
            }
        }
    }
}

#region 5. Summary (and non-zero job outcome if anything failed, so alerts fire)
Write-Output '================ SUMMARY ================'
$results | Format-Table -AutoSize | Out-String -Width 250 | Write-Output

if ($failures -gt 0) {
    throw "Run completed with $failures failure(s) - see warnings above."
}
Write-Output 'Run completed successfully.'
#endregion
