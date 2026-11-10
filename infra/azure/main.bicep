// Storage for monthly catalogue releases: one private container, versioned blobs,
// and optional data-plane access for one identity. Deploy with:
//   az deployment group create -g <resource-group> -f infra/azure/main.bicep \
//     -p storageAccountName=<3-24 lowercase letters and digits>
@description('Globally unique storage account name, 3 to 24 lowercase letters and digits.')
@minLength(3)
@maxLength(24)
param storageAccountName string

@description('Azure region. Defaults to the resource group location.')
param location string = resourceGroup().location

@description('Container that holds one folder per monthly snapshot date.')
param containerName string = 'dpd-releases'

@description('Days a deleted release stays recoverable.')
@minValue(1)
@maxValue(365)
param deleteRetentionDays int = 30

@description('Optional object ID of a user or managed identity that may read and write releases. Leave empty to grant nothing.')
param writerPrincipalId string = ''

@description('Type of the writer principal: ServicePrincipal, User or Group.')
@allowed([
  'ServicePrincipal'
  'User'
  'Group'
])
param writerPrincipalType string = 'ServicePrincipal'

@description('Use Entra ID only: set false to block account-key access once the writer identity is in place.')
param allowSharedKeyAccess bool = true

var blobDataContributorRoleId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
    allowSharedKeyAccess: allowSharedKeyAccess
    accessTier: 'Hot'
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'AzureServices'
    }
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storage
  name: 'default'
  properties: {
    isVersioningEnabled: true
    deleteRetentionPolicy: {
      enabled: true
      days: deleteRetentionDays
    }
    containerDeleteRetentionPolicy: {
      enabled: true
      days: deleteRetentionDays
    }
  }
}

resource releases 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: containerName
  properties: {
    publicAccess: 'None'
  }
}

resource writerRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (writerPrincipalId != '') {
  name: guid(releases.id, writerPrincipalId, blobDataContributorRoleId)
  scope: releases
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', blobDataContributorRoleId)
    principalId: writerPrincipalId
    principalType: writerPrincipalType
  }
}

output storageAccountName string = storage.name
output containerName string = releases.name
output blobEndpoint string = storage.properties.primaryEndpoints.blob
