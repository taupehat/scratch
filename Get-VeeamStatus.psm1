asnp "VeeamPSSnapIn" -ErrorAction SilentlyContinue

function Get-VeeamStatus {

    $failedjobs=@()

    # Traditional VMWare backup jobs
    foreach ($job in (Get-VBRJob -WarningAction:Ignore | where {$_.BackupPlatform.Platform -ne 'ELinuxPhysical' -and $_.BackupPlatform.Platform -ne 'EEndPoint'})) {
        $jobname = $job.LogNameMainPart
    	$status = $job.GetLastResult()
        
        if ($status -eq "Failed") {
            $failedjobs += "$jobname"
        }
    }

    # Linux agent jobs
    foreach ($cjob in (Get-VBRComputerBackupJob)) {
        $cjobname = $cjob.Name
        $cstatus = [Veeam.Backup.Core.CBackupJob]::FindLastSession($cjob.Id).Result
        
        if ($cstatus -eq "Failed") {
            $failedjobs += "$cjobname"
        }
    }

    # SOBR offload tasks
    # Use a hashtable to ensure uniqueness
    $jobRuns = @{}

    #This type corresponds to SOBR Offload job
    $sobrOffload = [Veeam.Backup.Model.EDbJobType]::ArchiveBackup 

    # Get the last day's worth of jobs and IMPORTANTLY sort by time: Veeam's default list order may not be deterministic
    $jobs = [Veeam.Backup.Core.CBackupSession]::GetByTypeAndTimeInterval($sobrOffload,(Get-Date).adddays(-1), (Get-Date).adddays(1)) | Select-Object -Property JobName, Result, EndTime | Sort-Object -Property EndTime

    # Now loop through the ordered list and overwrite the value for each key (jobName) ending with the most recent for each
    foreach ($job in $jobs) {
        $jobRuns[$job.JobName] = $job.Result
    }

    foreach ($key in $jobRuns.keys) {
        #Add some lag into this alert to give Veeam time to retry
        $now = (Get-Date)
        $jobTime = $job.EndTime
        $diff = New-TimeSpan -Start $jobTime -End $now

        if ($jobRuns[$key] -eq "Failed") {
            if($diff.Hours -ge 1){
                $failedjobs += "$key"
            }
        }
    }

    if ( $failedjobs -ne $null ) {
        foreach ( $fail in $failedjobs ) {
            Write-Host $fail
        }
    }
}

export-modulemember -function Get-VeeamStatus