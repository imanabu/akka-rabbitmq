#!/bin/env pwsh
git fetch
git checkout $Env:Branch
git pull
Remove-Item -Recurse -Force target
sbt publishLocal
