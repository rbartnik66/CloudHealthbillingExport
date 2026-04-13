# Input bindings are passed in via param block.
param($Timer, $chBillingDataExportin, $DBAuth)

# Get the current universal time in the default string format.
$currentUTCtime = (Get-Date).ToUniversalTime()

# The 'IsPastDue' property is 'true' when the current function invocation is later than scheduled.
if ($Timer.IsPastDue) {
    Write-Host "PowerShell timer is running late!"
}

# Write an information log with the current time.
Write-Host "PowerShell timer trigger function ran! TIME: $currentUTCtime"

# Configuration - should be in app settings
$chApiKey = $env:CHServiceAccountAPIKey
$chFunctionUrl = "https://managedsvc-cloud-reporting-pwshfunc.azurewebsites.net/api/chBillingDataExport-POST?code=4Q7ah73ztrYK9YoIo8dYxrqy7hGrIKr2A90fceUBOn6vAzFujLGpeg%3D%3D"
$chPutFunctionUrl = "https://managedsvc-cloud-reporting-pwshfunc.azurewebsites.net/api/chBillingDataExport-id-cloud-month-total-PUT"
$authToken = $DBAuth.auth_tokens.token


# Setup headers
$header = @{
    Authorization = "Bearer $chApiKey"
    "content-type" = "application/json"
    "Accept" = "application/json"
}

$authHeader = @{
    'Authorization' = $authToken
}

# Get all CH customers
Write-Host "Fetching CloudHealth customers..."
$CH_customers = @()
try {
    $GetCustomers = Invoke-WebRequest "https://chapi.cloudhealthtech.com/v2/customers?per_page=50" -Headers $header
    $CH_customers += ($GetCustomers.Content | ConvertFrom-Json).customers

    while ($GetCustomers.RelationLink["next"]) {
        $next = $GetCustomers.RelationLink["next"]
        $GetCustomers = Invoke-WebRequest $next -Headers $header
        $CH_customers += ($GetCustomers.Content | ConvertFrom-Json).customers
    }
    $CH_customers = $CH_customers | Sort-Object -Property name
    Write-Host "Found $($CH_customers.Count) CloudHealth customers"
} catch {
    Write-Host "Error fetching CloudHealth customers: $($_)"
    return
}
$culture = [System.Globalization.CultureInfo]::GetCultureInfo("en-US")
# Get existing customer IDs from database
$existingCustomerIds = @($chBillingDataExportin | ForEach-Object { $_.id })
Write-Host "Found $($existingCustomerIds.Count) existing customers in database"

# Process each customer
foreach ($customer in $CH_customers) {
    Write-Host "Processing customer: $($customer.name) (ID: $($customer.id))"

    Clear-Variable azure_spend, aws_spend, gcp_spend, gcp_historical_spend, azure_historical_spend, aws_historical_spend, ch_customer_data, azuremonthlySpend, awsmonthlySpend, gcpmonthlySpend -ErrorAction SilentlyContinue

    # Check if customer exists in database
    $isExistingCustomer = $existingCustomerIds -contains $customer.id

    if ($isExistingCustomer) {
        Write-Host "  -> Existing customer - fetching last month data only"

        # Fetch last month data


        $azure_spend = Invoke-RestMethod "https://chapi.cloudhealthtech.com/olap_reports/azure_cost/previous_billing_period?client_api_id=$($customer.id)&interval=monthly&measures[]=total_cost&dimensions[]=time&filters[]=time:select:-2&filters[]=Azure-Services:reject:1701&collapse_null_arrays=1" -Headers $header | select -ExpandProperty data | select -SkipLast 1 | select -Last 1
        $aws_spend = Invoke-RestMethod "https://chapi.cloudhealthtech.com/olap_reports/cost/history?client_api_id=$($customer.id)&interval=monthly&dimensions[]=time&measures[]=cost&filters[]=time:select:-2&filters[]=AWS-Service-Category:reject:marketplace&collapse_null_arrays=1" -Headers $header -ErrorAction SilentlyContinue | select -ExpandProperty data | select -SkipLast 1 | select -Last 1
        $gcp_spend = Invoke-RestMethod "https://chapi.cloudhealthtech.com/olap_reports/gcp_cost/history-v2?client_api_id=$($customer.id)&interval=monthly&dimensions[]=time&measures[]=cost&filters[]=time:select:-2&filters[]=GCP-Product-Category:reject:gcp_marketplace_third_party_sell" -Headers $header -ErrorAction SilentlyContinue | select -ExpandProperty data | select -SkipLast 1 | select -Last 1

        $lastMonth = (Get-Date).AddMonths(-1).ToString("MMMyyyy", [cultureinfo]::InvariantCulture)
        # Build single month data
        

        # Build customer object with last month data only
        $ch_customer_data = [PSCustomObject][ordered]@{
            name = $customer.name
            id = $customer.id
            tags = $customer.tags.value
            org_created = $customer.created_at
            trial_Expiration = $customer.trial_expiration
            is_trial = if ($customer.trial_expiration -eq $null){"No"}else{"Yes"}
            clouds = @{
                aws = [System.Collections.ArrayList]@()
                azure = [System.Collections.ArrayList]@()
                gcp = [System.Collections.ArrayList]@()
            }
            date_created = $(Get-Date -Format yyyy-MM-ddTHH:mm:ss)
        }

        # Add Azure last month data
        if ($azure_spend -and $azure_spend.Count -gt 0) {
            $azureTotal = [Math]::Round($azure_spend[0], 2)
            Write-Host "     Adding Azure: $lastMonth - $azureTotal"
            $ch_customer_data.clouds.Azure.Add([PSCustomObject]@{
                Month = $lastMonth
                Spend = $azureTotal
            })
        }

        # Add AWS last month data
        if ($aws_spend -and $aws_spend.Count -gt 0) {
            $awsTotal = [Math]::Round($aws_spend[0], 2)
            Write-Host "     Adding AWS: $lastMonth - $awsTotal"
            $ch_customer_data.clouds.AWS.Add([PSCustomObject]@{
                Month = $lastMonth
                Spend = $awsTotal
            })
        }

        # Add GCP last month data
        if ($gcp_spend -and $gcp_spend.Count -gt 0) {
            $gcpTotal = [Math]::Round($gcp_spend[0], 2)
            Write-Host "     Adding GCP: $lastMonth - $gcpTotal"
            $ch_customer_data.clouds.GCP.Add([PSCustomObject]@{
                Month = $lastMonth
                Spend = $gcpTotal
            })
        }

        # Send to PUT endpoint as body
        Write-Host "     Updating existing customer with last month data"
        try {
            $jsonBody = $ch_customer_data | ConvertTo-Json -Depth 20
            $response = invoke-restmethod $chPutFunctionUrl -Method PUT -Body $jsonBody -ContentType "application/json" -Headers $authHeader
            Write-Host "     Successfully updated customer: $($customer.name)"
        } catch {
            Write-Host "     Error updating customer: $($_)"
        }

    } else {
        Write-Host "  -> New customer - fetching all available historical data"
        
        # Fetch all available historical data (last 12 months)
        $azure_spend = Invoke-WebRequest (
        "https://chapi.cloudhealthtech.com/olap_reports/azure_cost/previous_billing_period" +
        "?client_api_id=$($customer.id)" +
        "&interval=monthly" +
        "&measures[]=total_cost" +
        "&dimensions[]=time" +
        "&filters[]=time:select:-12,-11,-10,-9,-8,-7,-6,-5,-4,-3,-2" +
        "&filters[]=Azure-Services:reject:1701" +
        "&collapse_null_arrays=1"
        ) -Headers $header

       
        $aws_spend = Invoke-WebRequest (
        "https://chapi.cloudhealthtech.com/olap_reports/cost/history" +
        "?client_api_id=$($customer.id)" +
        "&interval=monthly" +
        "&measures[]=cost" +
        "&dimensions[]=time" +
        "&filters[]=time:select:-12,-11,-10,-9,-8,-7,-6,-5,-4,-3,-2" +
        "&filters[]=AWS-Service-Category:reject:marketplace" +
        "&collapse_null_arrays=1"
        ) -Headers $header
        
        $gcp_spend = Invoke-WebRequest (
        "https://chapi.cloudhealthtech.com/olap_reports/gcp_cost/history-v2" +
        "?client_api_id=$($customer.id)" +
        "&interval=monthly" +
        "&measures[]=cost" +
        "&dimensions[]=time" +
        "&filters[]=time:select:-12,-11,-10,-9,-8,-7,-6,-5,-4,-3,-2" +
        "&filters[]=GCP-Product-Category:reject:gcp_marketplace_third_party_sell" +
        "&collapse_null_arrays=1"
        ) -Headers $header

        # Build customer object
        $ch_customer_data = [PSCustomObject][ordered]@{
            name = $customer.name
            id = $customer.id
            tags = $customer.tags.value
            org_created = $customer.created_at
            trial_Expiration = $customer.trial_expiration
            is_trial = if ($customer.trial_expiration -eq $null){"No"}else{"Yes"}
            clouds = @{
                aws = [System.Collections.ArrayList]@()
                azure = [System.Collections.ArrayList]@()
                gcp = [System.Collections.ArrayList]@()
            }
            date_created = $(Get-Date -Format yyyy-MM-ddTHH:mm:ss)
        }
        
        If ($azure_spend.StatusCode -eq '200'){
            $azure_spend = $azure_spend | ConvertFrom-Json

            $timeMembers = $azure_spend.dimensions[0].time 
            $rows = $azure_spend.data
            
            $azuremonthlySpend = for ($i = 0; $i -lt $timeMembers.Count; $i++) {

                $m = $timeMembers[$i]

                if ($m.name -eq "total") { continue }
                if ($m.excluded -eq $true) { continue }

                # If CloudHealth didn't return a row for this time bucket, skip it
                $row = $rows[$i]
                if ($null -eq $row) { continue }

                # Some reports return [ [value] ], others may return just value.
                $spend = if ($row -is [System.Array]) { $row[0] } else { $row }

                # If spend is still null, skip (optional)
                if ($null -eq $spend) { continue }

                $dt = [datetime]::ParseExact($m.name, "yyyy-MM", $null)

                [pscustomobject]@{
                    Month = $dt.ToString("MMMyyyy", $culture)   # Jan2026
                    Spend = [double]$spend
                }

        }
            # Add Azure monthly spend
            if ($azuremonthlySpend) {
                foreach ($item in $azuremonthlySpend) {
                    $spendValue = [Math]::Round($item.Spend, 2)
                    Write-Host "     Adding Azure: $($item.Month) - $spendValue"
                    $ch_customer_data.clouds.azure.Add([PSCustomObject]@{
                        Month = $item.Month
                        Spend = $spendValue
                    })
                }
            }
        }

        If ($aws_spend.StatusCode -eq '200'){
            $aws_spend = $aws_spend | ConvertFrom-Json

            $timeMembers = $aws_spend.dimensions[0].time
            $rows = $aws_spend.data

            $awsmonthlySpend = for ($i = 0; $i -lt $timeMembers.Count; $i++) {

                $m = $timeMembers[$i]

                if ($m.name -eq "total") { continue }
                if ($m.excluded -eq $true) { continue }

                # If CloudHealth didn't return a row for this time bucket, skip it
                $row = $rows[$i]
                if ($null -eq $row) { continue }

                # Some reports return [ [value] ], others may return just value.
                $spend = if ($row -is [System.Array]) { $row[0] } else { $row }

                # If spend is still null, skip (optional)
                if ($null -eq $spend) { continue }

                $dt = [datetime]::ParseExact($m.name, "yyyy-MM", $null)

                [pscustomobject]@{
                    Month = $dt.ToString("MMMyyyy", $culture)   # Jan2026
                    Spend = [double]$spend
                }
            }

            # Add AWS monthly spend
            if ($awsmonthlySpend) {
                foreach ($item in $awsmonthlySpend) {
                    $spendValue = [Math]::Round($item.Spend, 2)
                    Write-Host "     Adding AWS: $($item.Month) - $spendValue"
                    $ch_customer_data.clouds.aws.Add([PSCustomObject]@{
                        Month = $item.Month
                        Spend = $spendValue
                    })
                }
            }
        }

        If ($gcp_spend.StatusCode -eq '200'){
            $gcp_spend = $gcp_spend | ConvertFrom-Json

            $timeMembers = $gcp_spend.dimensions[0].time
            $rows = $gcp_spend.data

            $gcpmonthlySpend = for ($i = 0; $i -lt $timeMembers.Count; $i++) {

                $m = $timeMembers[$i]

                if ($m.name -eq "total") { continue }
                if ($m.excluded -eq $true) { continue }

                # If CloudHealth didn't return a row for this time bucket, skip it
                $row = $rows[$i]
                if ($null -eq $row) { continue }

                # Some reports return [ [value] ], others may return just value.
                $spend = if ($row -is [System.Array]) { $row[0] } else { $row }

                # If spend is still null, skip (optional)
                if ($null -eq $spend) { continue }

                $dt = [datetime]::ParseExact($m.name, "yyyy-MM", $null)

                [pscustomobject]@{
                    Month = $dt.ToString("MMMyyyy", $culture)   # Jan2026
                    Spend = [double]$spend
                }
            }

            # Add GCP monthly spend
            if ($gcpmonthlySpend) {
                foreach ($item in $gcpmonthlySpend) {
                    $spendValue = [Math]::Round($item.Spend, 2)
                    Write-Host "     Adding GCP: $($item.Month) - $spendValue"
                    $ch_customer_data.clouds.gcp.Add([PSCustomObject]@{
                        Month = $item.Month
                        Spend = $spendValue
                    })
                }
            }
        }

        # Send to POST endpoint
       
        try {
            $jsonBody = $ch_customer_data | ConvertTo-Json -Depth 20
            $response = invoke-restmethod $chFunctionUrl -Method POST -Body $jsonBody -ContentType "application/json"
            Write-Host "     Successfully created customer: $($customer.name)"
        } catch {
            Write-Host "     Error creating customer: $($_)"
        }
    }

    Start-Sleep -Seconds 1
}

Write-Host "Function completed at: $(Get-Date -Format yyyy-MM-ddTHH:mm:ss)"
