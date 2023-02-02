#!/bin/env pwsh
git fetch
git checkout $Branch
git pull
Remove-Item -Recurse -Force target
sbt publishLocal
