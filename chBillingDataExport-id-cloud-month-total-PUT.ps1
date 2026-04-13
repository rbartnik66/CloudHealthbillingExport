using namespace System.Net

param($Request, $TriggerMetadata, $backupDBAuth, $chBillingDataExportin)

Write-Host "PowerShell HTTP trigger function processed a request."

if (!$($Request.Headers.'Authorization')) {
    Push-OutputBinding -Name Response -Value ([HttpResponseContext]@{
        StatusCode = [HttpStatusCode]::Unauthorized
        Body = "Authorization not found in header"
    })
} else {
    if (($backupDBAuth.auth_tokens | Where-Object {$_.token -ccontains $($Request.Headers.'Authorization')}).endpoints -contains $TriggerMetadata.FunctionName) {

        # Handle the request body - it may already be an object or a JSON string
     
        $newData = $Request.Body
      
        # Check if record already exists
        $chBillingDataExportRecord = $chBillingDataExportin | Where-Object {$_.id -eq $newData.id}

        if ($chBillingDataExportRecord) {
            Write-Host "Existing record found for ID: $($newData.id). Merging data..."

            # Merge Azure data - only add months that don't already exist
            if ($newData.clouds.azure) {
                foreach ($month in $newData.clouds.azure) {
                    $existingMonth = $chBillingDataExportRecord.clouds.azure | Where-Object {$_.Month -eq $month.Month}
                    if (-not $existingMonth) {
                        Write-Host "Adding new Azure month: $($month.Month)"
                        $chBillingDataExportRecord.clouds.azure += [PSCustomObject]@{
                            Month = $month.Month
                            Spend = $month.Spend
                        }
                    } else {
                        Write-Host "Azure month $($month.Month) already exists. Updating spend..."
                        $existingMonth.Spend = $month.Spend
                    }
                }
                # Sort by date (chronologically)
                $chBillingDataExportRecord.clouds.azure = @($chBillingDataExportRecord.clouds.azure | Sort-Object {
                    [datetime]::ParseExact($_.Month, "MMMyyyy", [cultureinfo]::InvariantCulture)
                })
            }

            # Merge AWS data - only add months that don't already exist
            if ($newData.clouds.aws) {
                foreach ($month in $newData.clouds.aws) {
                    $existingMonth = $chBillingDataExportRecord.clouds.aws | Where-Object {$_.Month -eq $month.Month}
                    if (-not $existingMonth) {
                        Write-Host "Adding new AWS month: $($month.Month)"
                        $chBillingDataExportRecord.clouds.aws += [PSCustomObject]@{
                            Month = $month.Month
                            Spend = $month.Spend
                        }
                    } else {
                        Write-Host "AWS month $($month.Month) already exists. Updating spend..."
                        $existingMonth.Spend = $month.Spend
                    }
                }
                # Sort by date (chronologically)
                $chBillingDataExportRecord.clouds.aws = @($chBillingDataExportRecord.clouds.aws | Sort-Object {
                    [datetime]::ParseExact($_.Month, "MMMyyyy", [cultureinfo]::InvariantCulture)
                })
            }

            # Merge GCP data - only add months that don't already exist
            if ($newData.clouds.gcp) {
                foreach ($month in $newData.clouds.gcp) {
                    $existingMonth = $chBillingDataExportRecord.clouds.gcp | Where-Object {$_.Month -eq $month.Month}
                    if (-not $existingMonth) {
                        Write-Host "Adding new GCP month: $($month.Month)"
                        $chBillingDataExportRecord.clouds.gcp += [PSCustomObject]@{
                            Month = $month.Month
                            Spend = $month.Spend
                        }
                    } else {
                        Write-Host "GCP month $($month.Month) already exists. Updating spend..."
                        $existingMonth.Spend = $month.Spend
                    }
                }
                # Sort by date (chronologically)
                $chBillingDataExportRecord.clouds.gcp = @($chBillingDataExportRecord.clouds.gcp | Sort-Object {
                    [datetime]::ParseExact($_.Month, "MMMyyyy", [cultureinfo]::InvariantCulture)
                })
            }

            # Update other metadata if provided
            if ($newData.tags) {
                $chBillingDataExportRecord.tags = $newData.tags
            }
            if ($newData.trial_Expiration) {
                $chBillingDataExportRecord.trial_Expiration = $newData.trial_Expiration
            }
            $chBillingDataExportRecord.is_trial = $newData.is_trial

            # Update timestamp
            if ($chBillingDataExportRecord.date_updated) {
                $chBillingDataExportRecord.date_updated = $(Get-Date -Format yyyy-MM-ddTHH:mm:ss)
            } else {
                $chBillingDataExportRecord | Add-Member -NotePropertyName 'date_updated' -NotePropertyValue $(Get-Date -Format yyyy-MM-ddTHH:mm:ss) -Force
            }

            $recordToUpdate = $chBillingDataExportRecord

        } else {
            # New record - use the data as-is
            Write-Host "New record for ID: $($newData.id). Creating..."
            $newData | Add-Member -NotePropertyName 'date_updated' -NotePropertyValue $(Get-Date -Format yyyy-MM-ddTHH:mm:ss) -Force
            $recordToUpdate = $newData
        }

        try {
            Write-Host "Saving record to database..."
            $recordToUpdate | ConvertTo-Json -Depth 20
            Push-OutputBinding -Name outchBillingDataExport -Value $recordToUpdate
            Push-OutputBinding -Name Response -Value ([HttpResponseContext]@{
                StatusCode = [HttpStatusCode]::OK
                Body = $recordToUpdate
            })
        } catch {
            Write-Host "CH Billing Export DB Error: $($_)"
            Push-OutputBinding -Name Response -Value ([HttpResponseContext]@{
                StatusCode = [HttpStatusCode]::ServiceUnavailable
                Body = "The server encountered an internal error. Please retry the request."
            })
        }

    } else {
        $TriggerMetadata | ConvertTo-Json -Depth 20
        Push-OutputBinding -Name Response -Value ([HttpResponseContext]@{
            StatusCode = [HttpStatusCode]::Unauthorized
            Body = "Authorization token is not valid for this endpoint."
        })
    }
}
