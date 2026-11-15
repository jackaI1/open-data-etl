// Azure Data Factory for the monthly extract. The pipeline, datasets and trigger live as JSON in
// adf/ so they can be read on their own. This template adds what depends on the environment:
// the linked services (where the storage account and database are), the factory's permission
// to read the extract, and an email alert when a pipeline run fails after its retries.
// Deploy with (the password comes from your shell, never from a file in this repo):
//   az deployment group create -g <resource-group> -f infra/azure/data-factory.bicep \
//     -p factoryName=<unique-name> storageAccountName=<account from main.bicep> \
//        sqlServerFqdn=<from sql.bicep> sqlLoginName=<login> alertEmail=<you> \
//        sqlLoginPassword="$DPD_SQL_ADMIN_PASSWORD"

@description('Globally unique Data Factory name: 3 to 63 letters, digits and hyphens.')
@minLength(3)
@maxLength(63)
param factoryName string

@description('Azure region. Defaults to the resource group location.')
param location string = resourceGroup().location

@description('Storage account that holds the sql-extracts container, from main.bicep.')
param storageAccountName string

@description('Fully qualified name of the Azure SQL server, from sql.bicep.')
param sqlServerFqdn string

@description('Database that holds the mirror tables.')
param sqlDatabaseName string = 'dpd'

@description('SQL login the copy uses. It needs db_datawriter, and EXECUTE on the mirror procedures.')
param sqlLoginName string

@secure()
@description('Password for sqlLoginName. Stored by Data Factory as a secure string.')
param sqlLoginPassword string

@description('Address that receives an email when a pipeline run fails after its retries.')
param alertEmail string

var storageBlobDataReaderRoleId = '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1'

resource factory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: factoryName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {}
}

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: storageAccountName
}

resource factoryReadsExtracts 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, factory.id, storageBlobDataReaderRoleId)
  scope: storage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataReaderRoleId)
    principalId: factory.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource extractStorage 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  parent: factory
  name: 'ExtractBlobStorage'
  dependsOn: [
    factoryReadsExtracts
  ]
  properties: {
    type: 'AzureBlobStorage'
    typeProperties: {
      serviceEndpoint: 'https://${storageAccountName}.blob.${environment().suffixes.storage}/'
      accountKind: 'StorageV2'
    }
  }
}

resource sqlDatabase 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  parent: factory
  name: 'DpdSqlDatabase'
  properties: {
    type: 'AzureSqlDatabase'
    typeProperties: {
      connectionString: {
        type: 'SecureString'
        value: 'Server=tcp:${sqlServerFqdn},1433;Initial Catalog=${sqlDatabaseName};User ID=${sqlLoginName};Password=${sqlLoginPassword};Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;'
      }
    }
  }
}

resource extractCsvDataset 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  parent: factory
  name: 'ExtractCsv'
  dependsOn: [
    extractStorage
  ]
  properties: loadJsonContent('../../adf/datasets/ExtractCsv.json').properties
}

resource sqlTableDataset 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  parent: factory
  name: 'DpdSqlTable'
  dependsOn: [
    sqlDatabase
  ]
  properties: loadJsonContent('../../adf/datasets/DpdSqlTable.json').properties
}

resource monthlyPipeline 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  parent: factory
  name: 'MonthlyExtractToSql'
  dependsOn: [
    extractCsvDataset
    sqlTableDataset
  ]
  properties: loadJsonContent('../../adf/pipelines/MonthlyExtractToSql.json').properties
}

resource monthlyTrigger 'Microsoft.DataFactory/factories/triggers@2018-06-01' = {
  parent: factory
  name: 'MonthlySchedule'
  dependsOn: [
    monthlyPipeline
  ]
  properties: loadJsonContent('../../adf/triggers/MonthlySchedule.json').properties
}

resource alertGroup 'Microsoft.Insights/actionGroups@2023-01-01' = {
  name: '${factoryName}-failures'
  location: 'global'
  properties: {
    groupShortName: 'dpdadf'
    enabled: true
    emailReceivers: [
      {
        name: 'pipeline-owner'
        emailAddress: alertEmail
        useCommonAlertSchema: true
      }
    ]
  }
}

resource failedRunsAlert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: '${factoryName}-failed-runs'
  location: 'global'
  properties: {
    description: 'A monthly extract run failed after its retries.'
    severity: 2
    enabled: true
    scopes: [
      factory.id
    ]
    evaluationFrequency: 'PT15M'
    windowSize: 'PT1H'
    targetResourceType: 'Microsoft.DataFactory/factories'
    targetResourceRegion: location
    criteria: {
      'odata.type': 'Microsoft.Azure.Monitor.SingleResourceMultipleMetricCriteria'
      allOf: [
        {
          criterionType: 'StaticThresholdCriterion'
          name: 'PipelineFailedRuns'
          metricName: 'PipelineFailedRuns'
          metricNamespace: 'Microsoft.DataFactory/factories'
          operator: 'GreaterThan'
          threshold: 0
          timeAggregation: 'Total'
        }
      ]
    }
    actions: [
      {
        actionGroupId: alertGroup.id
      }
    ]
  }
}

output factoryName string = factory.name
output factoryPrincipalId string = factory.identity.principalId
