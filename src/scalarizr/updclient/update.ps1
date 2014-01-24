
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
$BackupCreatedLock = "$InstallDir\var\run\backup.created"
$InstalledVersion = ""
$LogFile = "$InstallDir\var\log\scalarizr_update.log"
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
        "Python27", "src" | foreach {
            for ($Cnt = 0; $Cnt -lt 5; $Cnt++) {
                Log "Renaming $InstallDir\$_ -> $_-$InstalledVersion"
                Rename-Item -Path "$InstallDir\$_" -NewName "$_-$InstalledVersion" -ErrorAction Continue
                if ($?) {
                    break
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
        "Python27", "src" | foreach {
            Rename-Item -Path "$InstallDir\$_-$InstalledVersion" -NewName $_
        }
        Remove-Item $BackupCreatedLock
    }
}

function Delete-SzrBackup {
    if (Test-Path $BackupCreatedLock) {
        Log "Cleanuping"
        "Python27", "src" | foreach {
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
    $Job = Start-Job {
        Stop-Service $Name
    }
    Wait-Job -Id $Job.Id -Timeout 30
    $Svs = Get-WmiObject -Class Win32_Service -Filter "Name = '$Name'"
    if ($Svs -and $Svs.ProcessId) {
        Stop-Process -Id $Svs.Processid -Force -ErrorAction Stop
    }
}

function Stop-SzrServices {
    Log "Stopping services"
    Stop-SzrService "ScalrUpdClient" 
    Stop-SzrService "Scalarizr"
    Start-Sleep -s 2
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
        finally {
            Start-SzrServices -ErrorAction Continue
            Remove-Item $PackageFile
        }
    }
    finally {
        $Msg = @()
        $Error | foreach { $Msg += [string]$_ }
        $Msg = $Msg | Select -Uniq
        $Status = @{
            error = $Msg -join "`n"; 
            state = $State
        } | ConvertTo-Json
        Log "Saving status: $Status"
        $Status > $StatusFile
    }
}

Main-Szr -ErrorAction Continue 5>&1 2>&1

