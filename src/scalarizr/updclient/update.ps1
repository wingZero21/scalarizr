
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
$UninstallRegKey = "hklm:\Software\Microsoft\Windows\CurrentVersion\Uninstall\Scalarizr"
$LogDir = "$InstallDir\var\log"
$RunDir = "$InstallDir\var\run"
$LogFile = "$LogDir\scalarizr_update_win.log"
$StatusFile = "$InstallDir\etc\private.d\update_win.status"
$BackupCreatedLock = "$RunDir\backup.created"
$DirsToBackup = @("src", "Python27")
$ServicesToOperate = @("ScalrUpdClient", "Scalarizr")
$InstalledVersion = ""
$State = "in-progress"
$PrevState = $Null


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
                    #handle "$InstallDir\$Name"
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
            $Path = "$InstallDir\$_-$InstalledVersion"
            $NewName = "$InstallDir\$_"
            if (Test-Path $NewName) {
                Remove-Item $NewName -Force -Recurse
            }
            if (Test-Path $Path) {
                Rename-Item -Path $Path -NewName $NewName
            }
        }
        Remove-Item $BackupCreatedLock

        Set-ItemProperty -Path $UninstallRegKey -Name DisplayVersion -Value $InstalledVersion
        Set-ItemProperty -Path $UninstallRegKey -Name DisplayName -Value "Scalarizr $InstalledVersion-1"
    }
}

function Delete-SzrBackup {
    if (Test-Path $BackupCreatedLock) {
        Log "Cleanuping"
        $DirsToBackup | foreach {
            $Path = "$InstallDir\$_-$InstalledVersion"
            if (Test-Path $Path) {
                Log "Remove $Path"
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
    Start-Sleep -s 2  # Give them time to think
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
    $ServicesToOperate | foreach {
        Stop-SzrService $_
    }
    Start-Sleep -s 2  # Give them time to shutdown
}

function Start-SzrServices {
param ($Certainly = $false)

    if ($Certainly -or (Get-Service "ScalrUpdClient" -ErrorAction SilentlyContinue)) {
        Log "Starting services"
        $ServicesToOperate | foreach {
            $Name = $_
            Start-Service $Name
            Log "Started $Name, sleeping"  
            Start-Sleep -s 5
            $Svs = Get-Service $Name
            Log "Status: $($Svs.Status)"
            if (-not $Svs.Status -eq "Running") {
                Throw "Serviсe started but failed a moment later: $Name"
            }
        }
    }
}

function Set-SzrState {
param ($State)
    $script:PrevState = $script:State
    $script:State = $State
}

function Save-SzrStatus {
    $Msg = @()
    $Error | foreach { $Msg += [string]$_ }
    $Msg = $Msg | Select -Uniq
    if ($Msg) {  
        # Empty arrays are not welcome: 
        # Exception calling "Reverse" with "1" argument(s): "Value cannot be null.
        [array]::Reverse($Msg)
    }
    $Version = $(Get-ItemProperty -Path $UninstallRegKey -Name DisplayVersion).DisplayVersion
    $Release = $(Get-ItemProperty -Path $UninstallRegKey -Name DisplayRelease).DisplayRelease
    $Installed = "$Version-$Release"

    $Status = @{
        error = $Msg -join "`n"; 
        state = $State;
        prev_state = $PrevState;
        installed = $Installed
    } | ConvertTo-Json
    Log "Saving status: $Status"
    Set-Content -Encoding Ascii -Path $StatusFile -Value $Status  
}

function Main-Szr {
    try {
        Set-SzrState "in-progress/prepare"
        if (Test-Path $StatusFile) {
            Remove-Item $StatusFile
        }
        if (Test-Path $InstallDir) {
            $script:InstalledVersion = Get-SzrVersion
        }
        Set-SzrState "in-progress/download-package"
        $PackageFile = Download-SzrPackage $URL
        Set-SzrState "in-progress/stop"
        Stop-SzrServices
        try {
            Create-SzrBackup
            try {
                Set-SzrState "in-progress/install"
                Install-SzrPackage $PackageFile
                if (-not (Test-Path "$InstallDir\src")) {
                    Throw "Installer completed without installing new files"
                }
                Set-SzrState "in-progress/restart"
                Save-SzrStatus
                Start-SzrServices -Сertainly
                Set-SzrState "completed"
            }
            catch {
                Write-Error $_ -ErrorAction Continue
                Stop-SzrServices
                Restore-SzrBackup
                Set-SzrState "rollbacked"
            }
            finally {
                Delete-SzrBackup
            }
        }
        catch {
            Write-Error $_ -ErrorAction Continue
            Set-SzrState "error"
        }
    }
    finally {
        try {
            Save-SzrStatus
            Start-SzrServices -ErrorAction Continue
            Remove-Item $PackageFile  
        } 
        catch {
            Write-Error $_ -ErrorAction Continue
        }     
    }
}

Main-Szr -ErrorAction Continue 5>&1 2>&1 | Tee-Object -Append $LogFile

