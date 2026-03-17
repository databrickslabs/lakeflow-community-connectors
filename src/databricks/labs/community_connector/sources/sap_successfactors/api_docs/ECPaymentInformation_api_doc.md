# Payment Information API Documentation

## Authorization

- **Method**: Basic Authentication
- **Header**: `Authorization: Basic base64(username:password)`
- **Username format**: `username@companyId`

## Object List

The following entities/objects are available in this API:

- Bank
- CustomPayType
- CustomPayTypeAssignment
- Payment Information Detail V3(Bolivia)
- Payment Information Detail V3(Czech Republic)
- Payment Information Detail V3(Paraguay)
- Payment Information Detail V3(Slovakia)
- Payment Information Detail V3(Tunisia)
- PaymentInformationDetailV3
- PaymentInformationDetailV3ARG
- PaymentInformationDetailV3BLR
- PaymentInformationDetailV3BRA
- PaymentInformationDetailV3CHL
- PaymentInformationDetailV3COL
- PaymentInformationDetailV3ECU
- PaymentInformationDetailV3ESP
- PaymentInformationDetailV3FRA
- PaymentInformationDetailV3GBR
- PaymentInformationDetailV3GHA
- PaymentInformationDetailV3IRQ
- PaymentInformationDetailV3ISR
- PaymentInformationDetailV3ITA
- PaymentInformationDetailV3JPN
- PaymentInformationDetailV3KEN
- PaymentInformationDetailV3MEX
- PaymentInformationDetailV3MKD
- PaymentInformationDetailV3MMR
- PaymentInformationDetailV3MOZ
- PaymentInformationDetailV3MWI
- PaymentInformationDetailV3NAM
- PaymentInformationDetailV3NGA
- PaymentInformationDetailV3NZL
- PaymentInformationDetailV3PER
- PaymentInformationDetailV3SUR
- PaymentInformationDetailV3SVN
- PaymentInformationDetailV3USA
- PaymentInformationDetailV3VEN
- PaymentInformationDetailV3ZAF
- PaymentInformationDetailV3ZWE
- PaymentInformationV3
- PaymentMethodAssignmentV3
- PaymentMethodV3

## Object Schema

Detailed schema for each entity:

### Bank

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| bankBranch | string | - | 60 | Yes |
| bankCountry | string | - | 128 | Yes |
| bankName | string | - | 128 | Yes |
| businessIdentifierCode | string | - | 20 | Yes |
| city | string | - | 255 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveStatus | string | - | 255 | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| postalCode | string | - | 255 | Yes |
| routingNumber | string | - | 255 | Yes |
| street | string | - | 255 | Yes |

### CustomPayType

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| externalName_de_DE | string | - | 255 | Yes |
| externalName_defaultValue | string | - | 255 | Yes |
| externalName_en_DEBUG | string | - | 255 | Yes |
| externalName_en_GB | string | - | 255 | Yes |
| externalName_en_US | string | - | 255 | Yes |
| externalName_es_ES | string | - | 255 | Yes |
| externalName_fr_FR | string | - | 255 | Yes |
| externalName_ja_JP | string | - | 255 | Yes |
| externalName_ko_KR | string | - | 255 | Yes |
| externalName_localized | string | - | 255 | Yes |
| externalName_nl_NL | string | - | 255 | Yes |
| externalName_pt_BR | string | - | 255 | Yes |
| externalName_pt_PT | string | - | 255 | Yes |
| externalName_ru_RU | string | - | 255 | Yes |
| externalName_zh_CN | string | - | 255 | Yes |
| externalName_zh_TW | string | - | 255 | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| standardPayType | string | - | 255 | Yes |
| toCustomPayTypeAssignment | Object | - | - | Yes |

### CustomPayTypeAssignment

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| CustomPayType_externalCode | string | - | 128 | No |
| country | string | - | 128 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### PaymentInformationDetailV3

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountNumber | string | - | 255 | Yes |
| accountOwner | string | - | 255 | Yes |
| amount | string | decimal | - | Yes |
| bank | string | - | 128 | Yes |
| bankCountry | string | - | 128 | Yes |
| bankNav | Ref(Bank) | - | - | No |
| businessIdentifierCode | string | - | 11 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| currency | string | - | 128 | Yes |
| customPayType | string | - | 128 | Yes |
| customPayTypeNav | Ref(CustomPayType) | - | - | No |
| externalCode | string | int64 | - | No |
| iban | string | - | 35 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| paySequence | string | int64 | - | Yes |
| payType | string | - | 255 | Yes |
| paymentMethod | string | - | 128 | Yes |
| paymentMethodNav | Ref(PaymentMethodV3) | - | - | No |
| percent | string | decimal | - | Yes |
| purpose | string | - | 40 | Yes |
| routingNumber | string | - | 255 | Yes |
| toPaymentInformationDetailV3ARG | Ref(PaymentInformationDetailV3ARG) | - | - | No |
| toPaymentInformationDetailV3BOL | Ref(PaymentInformationDetailV3BOL) | - | - | No |
| toPaymentInformationDetailV3BRA | Ref(PaymentInformationDetailV3BRA) | - | - | No |
| toPaymentInformationDetailV3CHL | Ref(PaymentInformationDetailV3CHL) | - | - | No |
| toPaymentInformationDetailV3COL | Ref(PaymentInformationDetailV3COL) | - | - | No |
| toPaymentInformationDetailV3CZE | Ref(PaymentInformationDetailV3CZE) | - | - | No |
| toPaymentInformationDetailV3ECU | Ref(PaymentInformationDetailV3ECU) | - | - | No |
| toPaymentInformationDetailV3ESP | Ref(PaymentInformationDetailV3ESP) | - | - | No |
| toPaymentInformationDetailV3FRA | Ref(PaymentInformationDetailV3FRA) | - | - | No |
| toPaymentInformationDetailV3GBR | Ref(PaymentInformationDetailV3GBR) | - | - | No |
| toPaymentInformationDetailV3ISR | Ref(PaymentInformationDetailV3ISR) | - | - | No |
| toPaymentInformationDetailV3ITA | Ref(PaymentInformationDetailV3ITA) | - | - | No |
| toPaymentInformationDetailV3JPN | Ref(PaymentInformationDetailV3JPN) | - | - | No |
| toPaymentInformationDetailV3KEN | Ref(PaymentInformationDetailV3KEN) | - | - | No |
| toPaymentInformationDetailV3MEX | Ref(PaymentInformationDetailV3MEX) | - | - | No |
| toPaymentInformationDetailV3NGA | Ref(PaymentInformationDetailV3NGA) | - | - | No |
| toPaymentInformationDetailV3NZL | Ref(PaymentInformationDetailV3NZL) | - | - | No |
| toPaymentInformationDetailV3PRY | Ref(PaymentInformationDetailV3PRY) | - | - | No |
| toPaymentInformationDetailV3SVK | Ref(PaymentInformationDetailV3SVK) | - | - | No |
| toPaymentInformationDetailV3TUN | Ref(PaymentInformationDetailV3TUN) | - | - | No |
| toPaymentInformationDetailV3USA | Ref(PaymentInformationDetailV3USA) | - | - | No |
| toPaymentInformationDetailV3VEN | Ref(PaymentInformationDetailV3VEN) | - | - | No |
| toPaymentInformationDetailV3ZAF | Ref(PaymentInformationDetailV3ZAF) | - | - | No |

### PaymentInformationDetailV3ARG

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 255 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |

### PaymentInformationDetailV3BLR

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 128 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### PaymentInformationDetailV3BOL

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 128 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### PaymentInformationDetailV3BRA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| checkDigit | string | - | 2 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |

### PaymentInformationDetailV3CHL

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| checkDigit | string | - | 2 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |

### PaymentInformationDetailV3COL

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 255 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| newAccountIdType | string | - | 255 | Yes |

### PaymentInformationDetailV3CZE

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| specificSymbol | string | int64 | - | Yes |

### PaymentInformationDetailV3ECU

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 255 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |

### PaymentInformationDetailV3ESP

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| checkDigit | string | - | 2 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |

### PaymentInformationDetailV3FRA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| checkDigit | string | - | 2 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |

### PaymentInformationDetailV3GBR

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| buildingSocietyRollNumber | string | - | 255 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |

### PaymentInformationDetailV3GHA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 128 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### PaymentInformationDetailV3IRQ

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 128 | Yes |
| checkDigit | string | - | 2 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### PaymentInformationDetailV3ISR

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| branchName | string | - | 255 | Yes |
| branchNumberCode | string | - | 255 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |

### PaymentInformationDetailV3ITA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| checkDigit | string | - | 1 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |

### PaymentInformationDetailV3JPN

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 255 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |

### PaymentInformationDetailV3KEN

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 255 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |

### PaymentInformationDetailV3MEX

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| checkDigit | string | - | 2 | Yes |
| clabe | string | - | 18 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |

### PaymentInformationDetailV3MKD

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 128 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### PaymentInformationDetailV3MMR

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 128 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### PaymentInformationDetailV3MOZ

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 128 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### PaymentInformationDetailV3MWI

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 128 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### PaymentInformationDetailV3NAM

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 128 | Yes |
| checkDigit | string | - | 2 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### PaymentInformationDetailV3NGA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 255 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |

### PaymentInformationDetailV3NZL

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| paymentReference | string | - | 38 | Yes |

### PaymentInformationDetailV3PER

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 255 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| paymentReference | string | - | 38 | Yes |

### PaymentInformationDetailV3PRY

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 128 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### PaymentInformationDetailV3SUR

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 128 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### PaymentInformationDetailV3SVK

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| constantSymbol | string | - | 4 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| specificSymbol | string | - | 10 | Yes |
| variableSymbol | string | - | 10 | Yes |

### PaymentInformationDetailV3SVN

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 128 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### PaymentInformationDetailV3TUN

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 128 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### PaymentInformationDetailV3USA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 255 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |

### PaymentInformationDetailV3VEN

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 255 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| paymentReference | string | - | 38 | Yes |

### PaymentInformationDetailV3ZAF

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountHolderRelationship | string | - | 255 | Yes |
| accountType | string | - | 255 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |

### PaymentInformationDetailV3ZWE

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentInformationDetailV3_externalCode | string | int64 | - | No |
| PaymentInformationV3_effectiveStartDate | string | - | - | No |
| PaymentInformationV3_worker | string | - | 100 | No |
| accountType | string | - | 128 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### PaymentInformationV3

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| jobCountry | string | - | 128 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| toPaymentInformationDetailV3 | Object | - | - | Yes |
| worker | string | - | 100 | No |

### PaymentMethodAssignmentV3

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PaymentMethodV3_externalCode | string | - | 128 | No |
| country | string | - | 128 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |

### PaymentMethodV3

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| externalName_de_DE | string | - | 255 | Yes |
| externalName_defaultValue | string | - | 255 | Yes |
| externalName_en_GB | string | - | 255 | Yes |
| externalName_en_US | string | - | 255 | Yes |
| externalName_es_ES | string | - | 255 | Yes |
| externalName_fr_FR | string | - | 255 | Yes |
| externalName_ja_JP | string | - | 255 | Yes |
| externalName_ko_KR | string | - | 255 | Yes |
| externalName_localized | string | - | 255 | Yes |
| externalName_nl_NL | string | - | 255 | Yes |
| externalName_pt_BR | string | - | 255 | Yes |
| externalName_pt_PT | string | - | 255 | Yes |
| externalName_ru_RU | string | - | 255 | Yes |
| externalName_zh_CN | string | - | 255 | Yes |
| externalName_zh_TW | string | - | 255 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| toPaymentMethodAssignmentV3 | Object | - | - | Yes |

## Object Primary Keys

Primary key fields identified for each entity:

- **Bank**: externalCode
- **CustomPayType**: externalCode
- **CustomPayTypeAssignment**: externalCode, CustomPayType_externalCode
- **PaymentInformationDetailV3**: mdfSystemRecordId, externalCode, mdfSystemEntityId, PaymentInformationV3_worker, mdfSystemVersionId, PaymentInformationV3_effectiveStartDate
- **PaymentInformationDetailV3ARG**: externalCode, mdfSystemRecordId, mdfSystemEntityId, PaymentInformationV3_worker, mdfSystemVersionId, PaymentInformationV3_effectiveStartDate, PaymentInformationDetailV3_externalCode
- **PaymentInformationDetailV3BLR**: PaymentInformationDetailV3_externalCode, PaymentInformationV3_worker, PaymentInformationV3_effectiveStartDate, externalCode
- **PaymentInformationDetailV3BOL**: PaymentInformationDetailV3_externalCode, PaymentInformationV3_worker, PaymentInformationV3_effectiveStartDate, externalCode
- **PaymentInformationDetailV3BRA**: externalCode, mdfSystemRecordId, mdfSystemEntityId, PaymentInformationV3_worker, mdfSystemVersionId, PaymentInformationV3_effectiveStartDate, PaymentInformationDetailV3_externalCode
- **PaymentInformationDetailV3CHL**: externalCode, mdfSystemRecordId, mdfSystemEntityId, PaymentInformationV3_worker, mdfSystemVersionId, PaymentInformationV3_effectiveStartDate, PaymentInformationDetailV3_externalCode
- **PaymentInformationDetailV3COL**: externalCode, mdfSystemRecordId, mdfSystemEntityId, PaymentInformationV3_worker, mdfSystemVersionId, PaymentInformationV3_effectiveStartDate, PaymentInformationDetailV3_externalCode
- **PaymentInformationDetailV3CZE**: PaymentInformationDetailV3_externalCode, PaymentInformationV3_worker, PaymentInformationV3_effectiveStartDate, externalCode
- **PaymentInformationDetailV3ECU**: externalCode, mdfSystemRecordId, mdfSystemEntityId, PaymentInformationV3_worker, mdfSystemVersionId, PaymentInformationV3_effectiveStartDate, PaymentInformationDetailV3_externalCode
- **PaymentInformationDetailV3ESP**: externalCode, mdfSystemRecordId, mdfSystemEntityId, PaymentInformationV3_worker, mdfSystemVersionId, PaymentInformationV3_effectiveStartDate, PaymentInformationDetailV3_externalCode
- **PaymentInformationDetailV3FRA**: externalCode, mdfSystemRecordId, mdfSystemEntityId, PaymentInformationV3_worker, mdfSystemVersionId, PaymentInformationV3_effectiveStartDate, PaymentInformationDetailV3_externalCode
- **PaymentInformationDetailV3GBR**: externalCode, mdfSystemRecordId, mdfSystemEntityId, PaymentInformationV3_worker, mdfSystemVersionId, PaymentInformationV3_effectiveStartDate, PaymentInformationDetailV3_externalCode
- **PaymentInformationDetailV3GHA**: PaymentInformationDetailV3_externalCode, PaymentInformationV3_worker, PaymentInformationV3_effectiveStartDate, externalCode
- **PaymentInformationDetailV3IRQ**: PaymentInformationDetailV3_externalCode, PaymentInformationV3_worker, PaymentInformationV3_effectiveStartDate, externalCode
- **PaymentInformationDetailV3ISR**: externalCode, mdfSystemRecordId, mdfSystemEntityId, PaymentInformationV3_worker, mdfSystemVersionId, PaymentInformationV3_effectiveStartDate, PaymentInformationDetailV3_externalCode
- **PaymentInformationDetailV3ITA**: externalCode, mdfSystemRecordId, mdfSystemEntityId, PaymentInformationV3_worker, mdfSystemVersionId, PaymentInformationV3_effectiveStartDate, PaymentInformationDetailV3_externalCode
- **PaymentInformationDetailV3JPN**: externalCode, mdfSystemRecordId, mdfSystemEntityId, PaymentInformationV3_worker, mdfSystemVersionId, PaymentInformationV3_effectiveStartDate, PaymentInformationDetailV3_externalCode
- **PaymentInformationDetailV3KEN**: externalCode, mdfSystemRecordId, mdfSystemEntityId, PaymentInformationV3_worker, mdfSystemVersionId, PaymentInformationV3_effectiveStartDate, PaymentInformationDetailV3_externalCode
- **PaymentInformationDetailV3MEX**: externalCode, mdfSystemRecordId, mdfSystemEntityId, PaymentInformationV3_worker, mdfSystemVersionId, PaymentInformationV3_effectiveStartDate, PaymentInformationDetailV3_externalCode
- **PaymentInformationDetailV3MKD**: PaymentInformationDetailV3_externalCode, PaymentInformationV3_worker, PaymentInformationV3_effectiveStartDate, externalCode
- **PaymentInformationDetailV3MMR**: PaymentInformationDetailV3_externalCode, PaymentInformationV3_worker, PaymentInformationV3_effectiveStartDate, externalCode
- **PaymentInformationDetailV3MOZ**: PaymentInformationDetailV3_externalCode, PaymentInformationV3_worker, PaymentInformationV3_effectiveStartDate, externalCode
- **PaymentInformationDetailV3MWI**: PaymentInformationDetailV3_externalCode, PaymentInformationV3_worker, PaymentInformationV3_effectiveStartDate, externalCode
- **PaymentInformationDetailV3NAM**: PaymentInformationDetailV3_externalCode, PaymentInformationV3_worker, PaymentInformationV3_effectiveStartDate, externalCode
- **PaymentInformationDetailV3NGA**: externalCode, mdfSystemRecordId, mdfSystemEntityId, PaymentInformationV3_worker, mdfSystemVersionId, PaymentInformationV3_effectiveStartDate, PaymentInformationDetailV3_externalCode
- **PaymentInformationDetailV3NZL**: externalCode, mdfSystemRecordId, mdfSystemEntityId, PaymentInformationV3_worker, mdfSystemVersionId, PaymentInformationV3_effectiveStartDate, PaymentInformationDetailV3_externalCode
- **PaymentInformationDetailV3PER**: PaymentInformationDetailV3_externalCode, PaymentInformationV3_worker, PaymentInformationV3_effectiveStartDate, externalCode
- **PaymentInformationDetailV3PRY**: PaymentInformationDetailV3_externalCode, PaymentInformationV3_worker, PaymentInformationV3_effectiveStartDate, externalCode
- **PaymentInformationDetailV3SUR**: PaymentInformationDetailV3_externalCode, PaymentInformationV3_worker, PaymentInformationV3_effectiveStartDate, externalCode
- **PaymentInformationDetailV3SVK**: PaymentInformationDetailV3_externalCode, PaymentInformationV3_worker, PaymentInformationV3_effectiveStartDate, externalCode
- **PaymentInformationDetailV3SVN**: PaymentInformationDetailV3_externalCode, PaymentInformationV3_worker, PaymentInformationV3_effectiveStartDate, externalCode
- **PaymentInformationDetailV3TUN**: PaymentInformationDetailV3_externalCode, PaymentInformationV3_worker, PaymentInformationV3_effectiveStartDate, externalCode
- **PaymentInformationDetailV3USA**: externalCode, mdfSystemRecordId, mdfSystemEntityId, PaymentInformationV3_worker, mdfSystemVersionId, PaymentInformationV3_effectiveStartDate, PaymentInformationDetailV3_externalCode
- **PaymentInformationDetailV3VEN**: externalCode, mdfSystemRecordId, mdfSystemEntityId, PaymentInformationV3_worker, mdfSystemVersionId, PaymentInformationV3_effectiveStartDate, PaymentInformationDetailV3_externalCode
- **PaymentInformationDetailV3ZAF**: externalCode, mdfSystemRecordId, mdfSystemEntityId, PaymentInformationV3_worker, mdfSystemVersionId, PaymentInformationV3_effectiveStartDate, PaymentInformationDetailV3_externalCode
- **PaymentInformationDetailV3ZWE**: PaymentInformationDetailV3_externalCode, PaymentInformationV3_worker, PaymentInformationV3_effectiveStartDate, externalCode
- **PaymentInformationV3**: mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, worker, effectiveStartDate
- **PaymentMethodAssignmentV3**: externalCode, mdfSystemRecordId, PaymentMethodV3_externalCode, mdfSystemEntityId, mdfSystemVersionId
- **PaymentMethodV3**: mdfSystemEntityId, externalCode, mdfSystemVersionId, mdfSystemRecordId

## Object's Ingestion Type

Ingestion types are determined based on API capabilities:


| Entity | Ingestion Type | Cursor Field |
|--------|---------------|--------------|
| Bank | cdc | lastModifiedDateTime |
| CustomPayType | cdc | lastModifiedDateTime |
| CustomPayTypeAssignment | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3 | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3ARG | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3BLR | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3BOL | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3BRA | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3CHL | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3COL | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3CZE | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3ECU | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3ESP | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3FRA | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3GBR | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3GHA | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3IRQ | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3ISR | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3ITA | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3JPN | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3KEN | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3MEX | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3MKD | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3MMR | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3MOZ | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3MWI | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3NAM | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3NGA | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3NZL | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3PER | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3PRY | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3SUR | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3SVK | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3SVN | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3TUN | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3USA | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3VEN | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3ZAF | cdc | lastModifiedDateTime |
| PaymentInformationDetailV3ZWE | cdc | lastModifiedDateTime |
| PaymentInformationV3 | cdc | lastModifiedDateTime |
| PaymentMethodAssignmentV3 | cdc | lastModifiedDateTime |
| PaymentMethodV3 | cdc | lastModifiedDateTime |

**Ingestion Type Definitions:**
- `cdc`: Change Data Capture - supports incremental reads using lastModifiedDateTime filter
- `snapshot`: Full snapshot - requires full table refresh each sync
- `cdc_with_deletes`: CDC with delete tracking (not commonly supported)

## Read API for Data Retrieval

### Base URL Pattern

```
https://{api-server}/odata/v2/{EntitySet}
```

### Available API Servers

Find your company's API server at: [SAP SuccessFactors API Servers](https://help.sap.com/viewer/d599f15995d348a1b45ba5603e2aba9b/LATEST/en-US/af2b8d5437494b12be88fe374eba75b6.html)

### Query Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `$select` | Select specific fields | `$select=userId,firstName,lastName` |
| `$filter` | Filter results | `$filter=lastModifiedDateTime ge datetime'2024-01-01T00:00:00'` |
| `$top` | Limit number of results | `$top=100` |
| `$skip` | Skip N results (pagination) | `$skip=100` |
| `$orderby` | Sort results | `$orderby=lastModifiedDateTime asc` |
| `$expand` | Include related entities | `$expand=employmentNav` |
| `$inlinecount` | Include total count | `$inlinecount=allpages` |

### Pagination

SAP SuccessFactors OData v2 uses server-side pagination:
- Default page size varies by entity
- Use `$top` and `$skip` for client-side pagination
- Response includes `__next` link for server-side pagination
- Always check for `d.results` array in response

### Example Requests

**Get all records from PaymentInformationDetailV3KEN:**
```http
GET https://{api-server}/odata/v2/PaymentInformationDetailV3KEN
Authorization: Basic {base64_credentials}
Accept: application/json
```

**Get records modified after a date (for CDC):**
```http
GET https://{api-server}/odata/v2/PaymentInformationDetailV3KEN?$filter=lastModifiedDateTime ge datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc
Authorization: Basic {base64_credentials}
Accept: application/json
```

**Get specific fields with pagination:**
```http
GET https://{api-server}/odata/v2/PaymentInformationDetailV3KEN?$select=externalCode,createdDateTime,lastModifiedDateTime&$top=100&$skip=0&$inlinecount=allpages
Authorization: Basic {base64_credentials}
Accept: application/json
```

### Response Format

```json
{
  "d": {
    "__count": "1000",
    "results": [
      {
        "__metadata": {
          "uri": "https://{api-server}/odata/v2/PaymentInformationDetailV3KEN('key')",
          "type": "SFOData.PaymentInformationDetailV3KEN"
        },
        "externalCode": "ABC123",
        "lastModifiedDateTime": "/Date(1704067200000)/"
      }
    ],
    "__next": "https://{api-server}/odata/v2/{EntitySet}?$skiptoken=..."
  }
}
```

## Field Type Mapping

| OData Type | Spark Type | Notes |
|------------|------------|-------|
| Edm.String | StringType | Default string type |
| Edm.Int32 | IntegerType | 32-bit integer |
| Edm.Int64 | LongType | 64-bit integer |
| Edm.Boolean | BooleanType | True/False |
| Edm.DateTime | TimestampType | Format: `/Date(milliseconds)/` |
| Edm.DateTimeOffset | TimestampType | DateTime with timezone |
| Edm.Decimal | DecimalType | Decimal numbers |
| Edm.Double | DoubleType | Double precision float |
| Edm.Binary | BinaryType | Base64 encoded |

### DateTime Parsing

SAP SuccessFactors returns dates in OData format:
- Format: `/Date(milliseconds)/` or `/Date(milliseconds+offset)/`
- Example: `/Date(1704067200000)/` = 2024-01-01T00:00:00Z
- Parse by extracting milliseconds and converting to timestamp

## Sources and References

- **SAP SuccessFactors API Spec**: `ECPaymentInformation.json`
- **SAP Help Portal**: [Payment Information on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/fb3d8aff79934730a8cd6934aec07392.html)
- **SAP API Business Hub**: [SAP SuccessFactors APIs](https://api.sap.com/package/SuccessFactorsFoundation/overview)
- **Authentication Guide**: [SAP SuccessFactors OData API Authentication](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/5c8bca0af1654b05a83193b2922dcee2.html)
