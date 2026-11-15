// Azure SQL Database on the free offer, the hosted copy of the reporting mirror.
// Deploy with (the password comes from your shell, never from a file in this repo):
//   az deployment group create -g <resource-group> -f infra/azure/sql.bicep \
//     -p sqlServerName=<unique-name> sqlAdminLogin=<login> sqlAdminPassword="$DPD_SQL_ADMIN_PASSWORD"
// The free offer covers one database per subscription. It auto-pauses when idle, so it
// stays free until it passes the monthly allowance.

@description('Globally unique logical server name: 3 to 63 lowercase letters, digits and hyphens.')
@minLength(3)
@maxLength(63)
param sqlServerName string

@description('Azure region. Defaults to the resource group location.')
param location string = resourceGroup().location

@description('Server admin login. Use it only to create the least-privilege loader user.')
param sqlAdminLogin string

@secure()
@description('Server admin password. Pass it at deploy time; it is never stored in this repo.')
param sqlAdminPassword string

@description('Database name the mirror tables live in.')
param databaseName string = 'dpd'

@description('Allow other Azure services, including Data Factory, to reach the server.')
param allowAzureServices bool = true

resource server 'Microsoft.Sql/servers@2023-05-01-preview' = {
  name: sqlServerName
  location: location
  properties: {
    administratorLogin: sqlAdminLogin
    administratorLoginPassword: sqlAdminPassword
    version: '12.0'
    minimalTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
  }
}

resource azureServicesRule 'Microsoft.Sql/servers/firewallRules@2023-05-01-preview' = if (allowAzureServices) {
  parent: server
  name: 'AllowAzureServices'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

resource database 'Microsoft.Sql/servers/databases@2023-05-01-preview' = {
  parent: server
  name: databaseName
  location: location
  sku: {
    name: 'GP_S_Gen5_1'
    tier: 'GeneralPurpose'
    family: 'Gen5'
    capacity: 1
  }
  properties: {
    useFreeLimit: true
    freeLimitExhaustionBehavior: 'AutoPause'
    autoPauseDelay: 60
    minCapacity: json('0.5')
    requestedBackupStorageRedundancy: 'Local'
    zoneRedundant: false
  }
}

output serverFqdn string = server.properties.fullyQualifiedDomainName
output databaseName string = database.name
