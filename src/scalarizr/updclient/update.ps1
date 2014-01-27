
# TODO: store update status as JSON to $StatusFile file

param (
    [string]$URL = $(throw "Specify package URL to install")
)

$DebugPreference = "Continue"
$ErrorActionPreference = "Stop"

function TmpName () {
param (
    [string]$Suffix
)
    return [System.IO.Path]::GetTempPath() + [System.Guid]::NewGuid().ToString() + $Suffix
}

$InstallDir = "C:\Program Files\Scalarizr"
$StatusFile = "$InstallDir\etc\private.d\update-win.status"
$DirsToBackup = @("src", "Python27")
$BackupCreatedLock = "$InstallDir\var\run\backup.created"
$InstalledVersion = ""
$LogFile = "$InstallDir\var\log\scalarizr_update_win.log"
$TmpLogFile = TmpName -Suffix ".log"
$BackupDir = ""


function Log() {
param (
    $Message
)
    #Add-Content $LogFile "$Message" 
    Write-Debug $Message
}

function Get-SzrVersion {
param (
    [string]$Path = $InstallDir
)
    return $(Get-Content "$Path\src\scalarizr\version").Trim()
}

function Download-SzrPackage {
param ($URL)

    $WC = New-Object System.Net.WebClient
    $Dst = TmpName -Suffix ".exe"
    Log "Downloading $url"
    $WC.DownloadFile($URL, $Dst)
    Log "Saved to $Dst"
    return $Dst
}

function Create-SzrBackup {
    if (Test-Path $InstallDir) {
        Log "Backuping current installation $(Get-SzrVersion)"
        $DirsToBackup | foreach {
            for ($Cnt = 0; $Cnt -lt 5; $Cnt++) {
                $Name = $_
                Log "Renaming $InstallDir\$_ -> $Name-$InstalledVersion"
                try {
                    Rename-Item -Path "$InstallDir\$Name" -NewName "$Name-$InstalledVersion"
                    break
                }
                catch {
                    Log "Finding a locker process"
                    handle "$InstallDir\$Name"
                    <#
                    Get-Process | foreach { 
                        $Proc = $_; 
                        #Log "===== PROCESS $Proc ===="
                        $_.Modules | foreach {
                            #Log "$($_.FileName)"
                            if($_.FileName -and ($_.FileName.IndexOf("$InstallDir\$Name") -eq 0)) { 
                                Log "Found the locker: " + $Proc.Name + " PID:" + $Proc.id
                            }
                        }
                    }
                    #>

                }
                Start-Sleep -s 1
            }
        }
        Echo $Null > $BackupCreatedLock
    }
}

function Restore-SzrBackup {
    if (Test-Path $BackupCreatedLock) {
        Log "Restoring previous installation from backup"
        $DirsToBackup | foreach {
            $NewName = $_
            $Path = "$InstallDir\$_-$InstalledVersion"
            if (Test-Path $Path) {
                Rename-Item -Path $Path -NewName $NewName
            }
        }
        Remove-Item $BackupCreatedLock
    }
}

function Delete-SzrBackup {
    if (Test-Path $BackupCreatedLock) {
        Log "Cleanuping"
        $DirsToBackup | foreach {
            $Path = "$InstallDir\$_-$InstalledVersion"
            if (Test-Path $Path) {
                Remove-Item $Path -Force -Recurse
            }
        }
        Remove-Item $BackupCreatedLock
    }
}

function Install-SzrPackage {
param ($PackageFile)
    Log "Starting installer"
    $Proc = Start-Process -Wait $PackageFile /S
    if ($Proc.ExitCode) {
        Throw "Scalarizr installer $(Split-Path -Leaf $PackageFile) exited with code: $($Proc.ExitCode)"
    }
    Log "Installer completed"
}

function Stop-SzrService {
param ($Name)
    Log "Stopping $Name"
    $Job = Start-Job -ScriptBlock {
        $DebugPreference = "Continue"
        $ErrorActionPreference = "Stop"
        $Name = $using:Name
        Stop-Service $using:Name
        Write-Debug "Stop-Service $Name completed"
        Get-WmiObject -Class Win32_Service -Filter "name = '$Name'"
    }
    Wait-Job $Job -Timeout 30
    if ($Job.State -eq "Running") {
        Log "Killing job $($Job.Id)"
        Stop-Job $Job
    } else {
        Receive-Job $Job
    }
    $Svs = Get-WmiObject -Class Win32_Service -Filter "Name = '$Name'"
    if ($Svs -and $Svs.ProcessId) {
        Stop-Process -Id $Svs.Processid -Force -ErrorAction Stop
    }
}

function Stop-SzrServices {
    Log "Stopping services"
    Stop-SzrService "ScalrUpdClient" 
    Stop-SzrService "Scalarizr"
    Start-Sleep -s 2  # Give them time to shutdown
}

function Start-SzrServices {
param ($Certainly = $false)

    if ($Certainly -or (Get-Service "ScalrUpdClient" -ErrorAction SilentlyContinue)) {
        Log "Starting services"
        Start-Service "ScalrUpdClient"  
        Start-Service "Scalarizr"
    }
}

function Main-Szr {
    try {
        $State = "prepare"
        if (Test-Path $StatusFile) {
            Remove-Item $StatusFile
        }
        if (Test-Path $InstallDir) {
            $script:InstalledVersion = Get-SzrVersion
        }
        $State = "download"
        $PackageFile = Download-SzrPackage $URL
        $State = "stop"
        Stop-SzrServices
        try {
            Create-SzrBackup
            try {
                $State = "install"
                Install-SzrPackage $PackageFile
                if (-not (Test-Path "$InstallDir\src")) {
                    Throw "Installer successfully completed without installing new files. Maybe this version was already installed?"
                }
                $State = "start"
                Start-SzrServices -Сertainly
                $State = "completed"
            }
            catch {
                Write-Error $_ -ErrorAction Continue
                Restore-SzrBackup
            }
            finally {
                Delete-SzrBackup
            }
        }
        catch {
            Write-Error $_ -ErrorAction Continue
        }
    }
    finally {
        $Msg = @()
        $Error | foreach { $Msg += [string]$_ }
        $Msg = $Msg | Select -Uniq
        [array]::Reverse($Msg)

        $Status = @{
            error = $Msg -join "`n"; 
            state = $State
        } | ConvertTo-Json
        Log "Saving status: $Status"
        Set-Content -Encoding Ascii -Path $StatusFile -Value $Status

        Start-SzrServices -ErrorAction Continue
        Remove-Item $PackageFile        
    }
}

Main-Szr -ErrorAction Continue 5>&1 2>&1 | Tee-Object -Append $LogFile

