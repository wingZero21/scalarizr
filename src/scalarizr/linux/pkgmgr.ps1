
param (
	[string]$Install = $(throw "Specify package URL to install")
)

$ErrorActionPreference = "Stop"

$InstallDir = "C:\\Program Files\\Scalarizr"
$RunCompletedFile = "$InstallDir\\var\\run\\pkgmgr.completed"
$LogFile = "$InstallDir\\var\\log\\scalarizr_update.log"
$BackupDir = $InstallDir + ".bak"
Start-Transcript -File $LogFile -Append


function Download-SzrPackage {
param ($URL)

	$WC = New-Object System.Net.WebClient
	$Dst = [System.IO.Path]::GetTempPath() + [System.Guid]::NewGuid().ToString() + ".exe"
	Write-Host "Downloading $url"
	$WC.DownloadFile($URL, $Dst)
	Write-Host "Saved to $Dst"
	return $Dst
}

function Create-SzrBackup {
	if (Test-Path $InstallDir) {
		Write-Host "Backuping current installation"
		Rename-Item $InstallDir $BackupDir
	}
}

function Restore-SzrBackup {
	if (Test-Path $BackupDir) {
		Write-Host "Restoring previous installation from backup"
		Rename-Item $BackupDir $InstallDir
	}
}

function Delete-SzrBackup {
	if (Test-Path $BackupDir) {
		Write-Host "Cleanuping"
		Remove-Item -Recurse -Force $BackupDir
	}
}

function Install-SzrPackage {
param ($PackageFile)
	Write-Host "Starting installer"
	$Proc = Start-Process -Wait $PackageFile /S
	if ($Proc.ExitCode) {
		Throw "Scalarizr installer $(Split-Path -Leaf $PackageFile) exited with code: $($Proc.ExitCode)"
	}
	Write-Host "Installer completed"
}

function Stop-SzrServices {
	Write-Host "Stopping services"
	Stop-Service "ScalrUpdClient" -ErrorAction SilentlyContinue
	Stop-Service "Scalarizr" -ErrorAction SilentlyContinue
}

function Start-SzrServices {
param ($Certainly = $false)

	if ($Certainly -or (Get-Service "ScalrUpdClient" -ErrorAction SilentlyContinue)) {
		Write-Host "Starting services"
		Start-Service "ScalrUpdClient"	
		Start-Service "Scalarizr"
	}
}


if (Test-Path $RunCompletedFile) {
	Remove-Item $RunCompletedFile
}
$PackageFile = Download-SzrPackage $Install
Stop-SzrServices
try {
	Create-SzrBackup
	try {
		Install-SzrPackage $PackageFile
		Start-SzrServices -Сertainly
		Echo $Null > $RunCompletedFile
	}
	catch {
		Restore-SzrBackup
		Throw $_
	}
	finally {
		Delete-SzrBackup
	}
}
finally {
	if ($Error.Count) {
		Write-Error $Error[0] -ErrorAction Continue
	}
	Start-SzrServices
	Remove-Item $PackageFile
}
