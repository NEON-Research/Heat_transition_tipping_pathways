# gradle-bootstrap.ps1 -- ONE-TIME: create the Gradle wrapper, then build.
#
# You only need this if `gradlew.bat` doesn't exist yet (it should be committed to the repo).
# It requires Gradle on PATH just this once to generate the wrapper; afterwards the wrapper
# self-manages the correct Gradle version and you just run:  .\gradlew.bat <task>
#
# Usage (from engine-java):  powershell -ExecutionPolicy Bypass -File .\gradle-bootstrap.ps1
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $MyInvocation.MyCommand.Path)

if (-not (Test-Path .\gradlew.bat)) {
    Write-Host "Generating the Gradle wrapper (one-time)..."
    # find a Gradle to bootstrap with: PATH, or one previously downloaded into .tooling
    $gradleCmd = (Get-Command gradle -ErrorAction SilentlyContinue).Source
    if (-not $gradleCmd) {
        $gradleCmd = (Get-ChildItem -Path .\.tooling -Recurse -Filter gradle.bat -ErrorAction SilentlyContinue |
                      Select-Object -First 1).FullName
    }
    if (-not $gradleCmd) {
        Write-Error "No Gradle found on PATH or in .tooling. Install Gradle (e.g. 'winget install Gradle.Gradle') and re-run, or generate the wrapper on any machine that has Gradle and commit gradlew.bat + gradle\wrapper\."
        exit 1
    }
    Write-Host "Using Gradle at: $gradleCmd"
    & $gradleCmd wrapper --gradle-version 8.7
}
.\gradlew.bat selfTest build
Write-Host "`nDone. From now on just use the wrapper, e.g.:"
Write-Host "    .\gradlew.bat selfTest"
Write-Host "    .\gradlew.bat cli --args=`"--real ..\data\stock\limburg_dwellings.csv --scenario baseline --iterations 20 --out java_baseline.csv`""
