# Foundation API Documentation

## Authorization

- **Method**: Basic Authentication
- **Header**: `Authorization: Basic base64(username:password)`
- **Username format**: `username@companyId`

## Object List

The following entities/objects are available in this API:

- BudgetGroup
- FOBusinessUnit
- FOCompany
- FOCorporateAddressDEFLT
- FOCostCenter
- FODepartment
- FODivision
- FODynamicRole
- FOEventReason
- FOFrequency
- FOGeozone
- FOJobClassLocalAUS
- FOJobClassLocalBRA
- FOJobClassLocalCAN
- FOJobClassLocalDEFLT
- FOJobClassLocalFRA
- FOJobClassLocalGBR
- FOJobClassLocalITA
- FOJobClassLocalUSA
- FOJobCode
- FOJobFunction
- FOLegalEntityLocalARG
- FOLegalEntityLocalDEFLT
- FOLegalEntityLocalDEU
- FOLegalEntityLocalESP
- FOLegalEntityLocalFRA
- FOLegalEntityLocalUSA
- FOLocation
- FOLocationGroup
- FOPayComponent
- FOPayComponentGroup
- FOPayGrade
- FOPayGroup
- FOPayRange
- FOWfConfig
- FOWfConfigStepApprover
- FoTranslation
- Job Classification for South Africa
- JobClassificationAUS
- JobClassificationBRA
- JobClassificationCAN
- JobClassificationCountry
- JobClassificationFRA
- JobClassificationGBR
- JobClassificationITA
- JobClassificationUSA
- LegalEntityARG
- LegalEntityBLR
- LegalEntityBOL
- LegalEntityCAN
- LegalEntityDEU
- LegalEntityESP
- LegalEntityFRA
- LegalEntityPRY
- LegalEntityRUS
- LegalEntitySAU
- LegalEntitySGP
- LegalEntitySVN
- LegalEntityTHA
- LegalEntityTUN
- LegalEntityUSA
- LocalizedData
- PayCalendar
- PayPeriod
- PayScaleArea
- PayScaleGroup
- PayScaleLevel
- PayScalePayComponent
- PayScaleType
- Periods
- Territory

## Object Schema

Detailed schema for each entity:

### BudgetGroup

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| description | string | - | 255 | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| effectiveStatus | string | - | 255 | Yes |
| externalCode | string | - | 128 | No |
| externalName | string | - | 128 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemCreatedBy | string | - | 255 | Yes |
| mdfSystemCreatedDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemLastModifiedBy | string | - | 255 | Yes |
| mdfSystemLastModifiedDate | string | - | - | Yes |
| mdfSystemLastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| parentBudgetGroup | Ref(BudgetGroup) | - | - | No |
| userId | string | - | 100 | Yes |

### FOBusinessUnit

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| description | string | - | 128 | Yes |
| descriptionTranslationNav | Ref(FoTranslation) | - | - | No |
| description_de_DE | string | - | 128 | Yes |
| description_defaultValue | string | - | 128 | Yes |
| description_en_GB | string | - | 128 | Yes |
| description_en_US | string | - | 128 | Yes |
| description_es_ES | string | - | 128 | Yes |
| description_fr_FR | string | - | 128 | Yes |
| description_ja_JP | string | - | 128 | Yes |
| description_ko_KR | string | - | 128 | Yes |
| description_localized | string | - | 128 | Yes |
| description_nl_NL | string | - | 128 | Yes |
| description_pt_BR | string | - | 128 | Yes |
| description_pt_PT | string | - | 128 | Yes |
| description_ru_RU | string | - | 128 | Yes |
| description_zh_CN | string | - | 128 | Yes |
| description_zh_TW | string | - | 128 | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| headOfUnit | string | - | 100 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| name | string | - | 32 | Yes |
| nameTranslationNav | Ref(FoTranslation) | - | - | No |
| name_de_DE | string | - | 32 | Yes |
| name_defaultValue | string | - | 32 | Yes |
| name_en_GB | string | - | 32 | Yes |
| name_en_US | string | - | 32 | Yes |
| name_es_ES | string | - | 32 | Yes |
| name_fr_FR | string | - | 32 | Yes |
| name_ja_JP | string | - | 32 | Yes |
| name_ko_KR | string | - | 32 | Yes |
| name_localized | string | - | 32 | Yes |
| name_nl_NL | string | - | 32 | Yes |
| name_pt_BR | string | - | 32 | Yes |
| name_pt_PT | string | - | 32 | Yes |
| name_ru_RU | string | - | 32 | Yes |
| name_zh_CN | string | - | 32 | Yes |
| name_zh_TW | string | - | 32 | Yes |
| startDate | string | - | - | No |
| status | string | - | 255 | Yes |

### FOCompany

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| country | string | - | 128 | Yes |
| countryNav | Ref(Territory) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| currency | string | - | 128 | Yes |
| defaultLocation | string | - | 32 | Yes |
| defaultLocationNav | Ref(FOLocation) | - | - | No |
| defaultPayGroup | string | - | 128 | Yes |
| defaultPayGroupNav | Ref(FOPayGroup) | - | - | No |
| description | string | - | 128 | Yes |
| descriptionTranslationNav | Ref(FoTranslation) | - | - | No |
| description_de_DE | string | - | 128 | Yes |
| description_defaultValue | string | - | 128 | Yes |
| description_en_GB | string | - | 128 | Yes |
| description_en_US | string | - | 128 | Yes |
| description_es_ES | string | - | 128 | Yes |
| description_fr_FR | string | - | 128 | Yes |
| description_ja_JP | string | - | 128 | Yes |
| description_ko_KR | string | - | 128 | Yes |
| description_localized | string | - | 128 | Yes |
| description_nl_NL | string | - | 128 | Yes |
| description_pt_BR | string | - | 128 | Yes |
| description_pt_PT | string | - | 128 | Yes |
| description_ru_RU | string | - | 128 | Yes |
| description_zh_CN | string | - | 128 | Yes |
| description_zh_TW | string | - | 128 | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| localNavARG | Ref(FOLegalEntityLocalARG) | - | - | No |
| localNavDEFLT | Ref(FOLegalEntityLocalDEFLT) | - | - | No |
| localNavDEU | Ref(FOLegalEntityLocalDEU) | - | - | No |
| localNavESP | Ref(FOLegalEntityLocalESP) | - | - | No |
| localNavFRA | Ref(FOLegalEntityLocalFRA) | - | - | No |
| localNavUSA | Ref(FOLegalEntityLocalUSA) | - | - | No |
| name | string | - | 32 | Yes |
| nameTranslationNav | Ref(FoTranslation) | - | - | No |
| name_de_DE | string | - | 32 | Yes |
| name_defaultValue | string | - | 32 | Yes |
| name_en_GB | string | - | 32 | Yes |
| name_en_US | string | - | 32 | Yes |
| name_es_ES | string | - | 32 | Yes |
| name_fr_FR | string | - | 32 | Yes |
| name_ja_JP | string | - | 32 | Yes |
| name_ko_KR | string | - | 32 | Yes |
| name_localized | string | - | 32 | Yes |
| name_nl_NL | string | - | 32 | Yes |
| name_pt_BR | string | - | 32 | Yes |
| name_pt_PT | string | - | 32 | Yes |
| name_ru_RU | string | - | 32 | Yes |
| name_zh_CN | string | - | 32 | Yes |
| name_zh_TW | string | - | 32 | Yes |
| standardHours | number | double | - | Yes |
| startDate | string | - | - | No |
| status | string | - | 255 | Yes |
| toLegalEntityARG | Ref(LegalEntityARG) | - | - | No |
| toLegalEntityBLR | Ref(LegalEntityBLR) | - | - | No |
| toLegalEntityBOL | Ref(LegalEntityBOL) | - | - | No |
| toLegalEntityCAN | Ref(LegalEntityCAN) | - | - | No |
| toLegalEntityDEU | Ref(LegalEntityDEU) | - | - | No |
| toLegalEntityESP | Ref(LegalEntityESP) | - | - | No |
| toLegalEntityFRA | Ref(LegalEntityFRA) | - | - | No |
| toLegalEntityPRY | Ref(LegalEntityPRY) | - | - | No |
| toLegalEntityRUS | Ref(LegalEntityRUS) | - | - | No |
| toLegalEntitySAU | Ref(LegalEntitySAU) | - | - | No |
| toLegalEntitySGP | Ref(LegalEntitySGP) | - | - | No |
| toLegalEntitySVN | Ref(LegalEntitySVN) | - | - | No |
| toLegalEntityTHA | Ref(LegalEntityTHA) | - | - | No |
| toLegalEntityTUN | Ref(LegalEntityTUN) | - | - | No |
| toLegalEntityUSA | Ref(LegalEntityUSA) | - | - | No |

### FOCorporateAddressDEFLT

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| address1 | string | - | 256 | Yes |
| address2 | string | - | 256 | Yes |
| address3 | string | - | 256 | Yes |
| address4 | string | - | 256 | Yes |
| address5 | string | - | 256 | Yes |
| addressId | string | decimal | - | No |
| city | string | - | 256 | Yes |
| country | string | - | 256 | Yes |
| countryNav | Ref(Territory) | - | - | No |
| county | string | - | 256 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| endDate | string | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| province | string | - | 256 | Yes |
| startDate | string | - | - | Yes |
| state | string | - | 256 | Yes |
| zipCode | string | - | 256 | Yes |

### FOCostCenter

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| costcenterExternalObjectID | string | - | 40 | Yes |
| costcenterManager | string | - | 100 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| cust_LegalEntity | Object | - | - | Yes |
| cust_LegalEntityProp | string | - | - | Yes |
| description | string | - | 128 | Yes |
| descriptionTranslationNav | Ref(FoTranslation) | - | - | No |
| description_de_DE | string | - | 128 | Yes |
| description_defaultValue | string | - | 128 | Yes |
| description_en_GB | string | - | 128 | Yes |
| description_en_US | string | - | 128 | Yes |
| description_es_ES | string | - | 128 | Yes |
| description_fr_FR | string | - | 128 | Yes |
| description_ja_JP | string | - | 128 | Yes |
| description_ko_KR | string | - | 128 | Yes |
| description_localized | string | - | 128 | Yes |
| description_nl_NL | string | - | 128 | Yes |
| description_pt_BR | string | - | 128 | Yes |
| description_pt_PT | string | - | 128 | Yes |
| description_ru_RU | string | - | 128 | Yes |
| description_zh_CN | string | - | 128 | Yes |
| description_zh_TW | string | - | 128 | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| glStatementCode | string | - | 32 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| name | string | - | 90 | Yes |
| nameTranslationNav | Ref(FoTranslation) | - | - | No |
| name_de_DE | string | - | 90 | Yes |
| name_defaultValue | string | - | 90 | Yes |
| name_en_GB | string | - | 90 | Yes |
| name_en_US | string | - | 90 | Yes |
| name_es_ES | string | - | 90 | Yes |
| name_fr_FR | string | - | 90 | Yes |
| name_ja_JP | string | - | 90 | Yes |
| name_ko_KR | string | - | 90 | Yes |
| name_localized | string | - | 90 | Yes |
| name_nl_NL | string | - | 90 | Yes |
| name_pt_BR | string | - | 90 | Yes |
| name_pt_PT | string | - | 90 | Yes |
| name_ru_RU | string | - | 90 | Yes |
| name_zh_CN | string | - | 90 | Yes |
| name_zh_TW | string | - | 90 | Yes |
| parent | string | - | 128 | Yes |
| parentNav | Ref(FOCostCenter) | - | - | No |
| startDate | string | - | - | No |
| status | string | - | 255 | Yes |

### FODepartment

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| costCenter | string | - | 128 | Yes |
| costCenterNav | Ref(FOCostCenter) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| cust_toDivision | Object | - | - | Yes |
| cust_toLegalEntity | Object | - | - | Yes |
| cust_toLegalEntityProp | string | - | - | Yes |
| description | string | - | 128 | Yes |
| descriptionTranslationNav | Ref(FoTranslation) | - | - | No |
| description_de_DE | string | - | 128 | Yes |
| description_defaultValue | string | - | 128 | Yes |
| description_en_GB | string | - | 128 | Yes |
| description_en_US | string | - | 128 | Yes |
| description_es_ES | string | - | 128 | Yes |
| description_fr_FR | string | - | 128 | Yes |
| description_ja_JP | string | - | 128 | Yes |
| description_ko_KR | string | - | 128 | Yes |
| description_localized | string | - | 128 | Yes |
| description_nl_NL | string | - | 128 | Yes |
| description_pt_BR | string | - | 128 | Yes |
| description_pt_PT | string | - | 128 | Yes |
| description_ru_RU | string | - | 128 | Yes |
| description_zh_CN | string | - | 128 | Yes |
| description_zh_TW | string | - | 128 | Yes |
| divisionFlx | string | - | - | Yes |
| divisionFlxNav | Object | - | - | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| headOfUnit | string | - | 100 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| name | string | - | 32 | Yes |
| nameTranslationNav | Ref(FoTranslation) | - | - | No |
| name_de_DE | string | - | 32 | Yes |
| name_defaultValue | string | - | 32 | Yes |
| name_en_GB | string | - | 32 | Yes |
| name_en_US | string | - | 32 | Yes |
| name_es_ES | string | - | 32 | Yes |
| name_fr_FR | string | - | 32 | Yes |
| name_ja_JP | string | - | 32 | Yes |
| name_ko_KR | string | - | 32 | Yes |
| name_localized | string | - | 32 | Yes |
| name_nl_NL | string | - | 32 | Yes |
| name_pt_BR | string | - | 32 | Yes |
| name_pt_PT | string | - | 32 | Yes |
| name_ru_RU | string | - | 32 | Yes |
| name_zh_CN | string | - | 32 | Yes |
| name_zh_TW | string | - | 32 | Yes |
| parent | string | - | 128 | Yes |
| parentNav | Ref(FODepartment) | - | - | No |
| startDate | string | - | - | No |
| status | string | - | 255 | Yes |

### FODivision

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| businessUnitFlx | string | - | - | Yes |
| businessUnitFlxNav | Object | - | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| cust_toBusinessUnit | Object | - | - | Yes |
| description | string | - | 128 | Yes |
| descriptionTranslationNav | Ref(FoTranslation) | - | - | No |
| description_de_DE | string | - | 128 | Yes |
| description_defaultValue | string | - | 128 | Yes |
| description_en_GB | string | - | 128 | Yes |
| description_en_US | string | - | 128 | Yes |
| description_es_ES | string | - | 128 | Yes |
| description_fr_FR | string | - | 128 | Yes |
| description_ja_JP | string | - | 128 | Yes |
| description_ko_KR | string | - | 128 | Yes |
| description_localized | string | - | 128 | Yes |
| description_nl_NL | string | - | 128 | Yes |
| description_pt_BR | string | - | 128 | Yes |
| description_pt_PT | string | - | 128 | Yes |
| description_ru_RU | string | - | 128 | Yes |
| description_zh_CN | string | - | 128 | Yes |
| description_zh_TW | string | - | 128 | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| headOfUnit | string | - | 100 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| name | string | - | 32 | Yes |
| nameTranslationNav | Ref(FoTranslation) | - | - | No |
| name_de_DE | string | - | 32 | Yes |
| name_defaultValue | string | - | 32 | Yes |
| name_en_GB | string | - | 32 | Yes |
| name_en_US | string | - | 32 | Yes |
| name_es_ES | string | - | 32 | Yes |
| name_fr_FR | string | - | 32 | Yes |
| name_ja_JP | string | - | 32 | Yes |
| name_ko_KR | string | - | 32 | Yes |
| name_localized | string | - | 32 | Yes |
| name_nl_NL | string | - | 32 | Yes |
| name_pt_BR | string | - | 32 | Yes |
| name_pt_PT | string | - | 32 | Yes |
| name_ru_RU | string | - | 32 | Yes |
| name_zh_CN | string | - | 32 | Yes |
| name_zh_TW | string | - | 32 | Yes |
| parent | string | - | 128 | Yes |
| parentNav | Ref(FODivision) | - | - | No |
| startDate | string | - | - | No |
| status | string | - | 255 | Yes |

### FODynamicRole

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| businessUnit | string | - | - | Yes |
| businessUnitNav | Object | - | - | Yes |
| company | string | - | - | Yes |
| companyNav | Object | - | - | Yes |
| costCenter | string | - | - | Yes |
| costCenterNav | Object | - | - | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| department | string | - | - | Yes |
| departmentNav | Object | - | - | Yes |
| description | string | - | 128 | Yes |
| descriptionTranslationNav | Ref(FoTranslation) | - | - | No |
| division | string | - | - | Yes |
| divisionNav | Object | - | - | Yes |
| dynamicGroup | string | - | - | Yes |
| dynamicRoleAssignmentId | string | decimal | - | No |
| eventReason | string | - | - | Yes |
| eventReasonNav | Object | - | - | Yes |
| externalCode | string | - | 32 | No |
| jobCode | string | - | - | Yes |
| jobCodeNav | Object | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| location | string | - | - | Yes |
| locationNav | Object | - | - | Yes |
| name | string | - | 32 | Yes |
| nameTranslationNav | Ref(FoTranslation) | - | - | No |
| payGrade | string | - | - | Yes |
| payGradeNav | Object | - | - | Yes |
| payGroup | string | - | - | Yes |
| payGroupNav | Object | - | - | Yes |
| person | string | - | - | Yes |
| position | string | - | - | Yes |
| resolverType | string | - | - | Yes |

### FOEventReason

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| description | string | - | 128 | Yes |
| descriptionTranslationNav | Ref(FoTranslation) | - | - | No |
| emplStatus | string | - | 45 | Yes |
| endDate | string | - | - | Yes |
| event | string | - | 45 | Yes |
| externalCode | string | - | 32 | No |
| implicitPositionAction | string | int64 | - | Yes |
| includeInWorkExperience | boolean | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| name | string | - | 32 | Yes |
| nameTranslationNav | Ref(FoTranslation) | - | - | No |
| payrollEvent | string | - | 4 | Yes |
| startDate | string | - | - | No |
| status | string | - | - | Yes |

### FOFrequency

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| annualizationFactor | number | double | - | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| description | string | - | 128 | Yes |
| descriptionTranslationNav | Ref(FoTranslation) | - | - | No |
| externalCode | string | - | 32 | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| name | string | - | 32 | Yes |
| nameTranslationNav | Ref(FoTranslation) | - | - | No |

### FOGeozone

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| adjustmentPercentage | string | decimal | - | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| description | string | - | 128 | Yes |
| descriptionTranslationNav | Ref(FoTranslation) | - | - | No |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| name | string | - | 32 | Yes |
| nameTranslationNav | Ref(FoTranslation) | - | - | No |
| startDate | string | - | - | No |
| status | string | - | - | Yes |

### FOJobClassLocalAUS

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| country | string | - | 128 | No |
| countryNav | Ref(Territory) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| genericString1 | string | - | 32 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| startDate | string | - | - | No |
| status | string | - | 255 | Yes |

### FOJobClassLocalBRA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| country | string | - | 128 | No |
| countryNav | Ref(Territory) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| genericString1 | string | - | 32 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| startDate | string | - | - | No |
| status | string | - | 255 | Yes |

### FOJobClassLocalCAN

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| country | string | - | 128 | No |
| countryNav | Ref(Territory) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| genericString1 | string | - | 255 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| startDate | string | - | - | No |
| status | string | - | 255 | Yes |

### FOJobClassLocalDEFLT

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| country | string | - | - | No |
| countryNav | Ref(Territory) | - | - | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| startDate | string | - | - | No |
| status | string | - | - | Yes |

### FOJobClassLocalFRA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| country | string | - | 128 | No |
| countryNav | Ref(Territory) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| genericNumber2 | string | int64 | - | Yes |
| genericString1 | string | - | 45 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| startDate | string | - | - | No |
| status | string | - | 255 | Yes |

### FOJobClassLocalGBR

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| country | string | - | 128 | No |
| countryNav | Ref(Territory) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| cust_long1 | string | - | 128 | Yes |
| customLong1 | string | int64 | - | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| genericNumber1 | string | int64 | - | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| startDate | string | - | - | No |
| status | string | - | 255 | Yes |

### FOJobClassLocalITA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| country | string | - | 128 | No |
| countryNav | Ref(Territory) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| genericNumber1 | string | int64 | - | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| startDate | string | - | - | No |
| status | string | - | 255 | Yes |

### FOJobClassLocalUSA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| country | string | - | 128 | No |
| countryNav | Ref(Territory) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| eeo1JobCategory | string | - | 128 | Yes |
| eeo4JobCategory | string | - | 128 | Yes |
| eeo5JobCategory | string | - | 128 | Yes |
| eeo6JobCategory | string | - | 128 | Yes |
| eeoJobGroup | string | - | 128 | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| flsaStatusUSA | string | - | 128 | Yes |
| genericNumber1 | string | int64 | - | Yes |
| genericNumber2 | string | int64 | - | Yes |
| genericNumber3 | string | int64 | - | Yes |
| genericNumber4 | string | int64 | - | Yes |
| genericNumber5 | string | int64 | - | Yes |
| genericNumber6 | string | int64 | - | Yes |
| genericString1 | string | - | 32 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| startDate | string | - | - | No |
| status | string | - | 255 | Yes |

### FOJobCode

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| cust_string10 | string | - | 128 | Yes |
| cust_string1_de_DE | string | - | 70 | Yes |
| cust_string1_defaultValue | string | - | 70 | Yes |
| cust_string1_en_GB | string | - | 70 | Yes |
| cust_string1_en_US | string | - | 70 | Yes |
| cust_string1_es_ES | string | - | 70 | Yes |
| cust_string1_fr_FR | string | - | 70 | Yes |
| cust_string1_ja_JP | string | - | 70 | Yes |
| cust_string1_ko_KR | string | - | 70 | Yes |
| cust_string1_localized | string | - | 70 | Yes |
| cust_string1_nl_NL | string | - | 70 | Yes |
| cust_string1_pt_BR | string | - | 70 | Yes |
| cust_string1_pt_PT | string | - | 70 | Yes |
| cust_string1_ru_RU | string | - | 70 | Yes |
| cust_string1_zh_CN | string | - | 70 | Yes |
| cust_string1_zh_TW | string | - | 70 | Yes |
| cust_string2_de_DE | string | - | 32 | Yes |
| cust_string2_defaultValue | string | - | 32 | Yes |
| cust_string2_en_GB | string | - | 32 | Yes |
| cust_string2_en_US | string | - | 32 | Yes |
| cust_string2_es_ES | string | - | 32 | Yes |
| cust_string2_fr_FR | string | - | 32 | Yes |
| cust_string2_ja_JP | string | - | 32 | Yes |
| cust_string2_ko_KR | string | - | 32 | Yes |
| cust_string2_localized | string | - | 32 | Yes |
| cust_string2_nl_NL | string | - | 32 | Yes |
| cust_string2_pt_BR | string | - | 32 | Yes |
| cust_string2_pt_PT | string | - | 32 | Yes |
| cust_string2_ru_RU | string | - | 32 | Yes |
| cust_string2_zh_CN | string | - | 32 | Yes |
| cust_string2_zh_TW | string | - | 32 | Yes |
| cust_string3 | string | - | 128 | Yes |
| cust_string4 | string | - | 128 | Yes |
| cust_string5 | string | - | 128 | Yes |
| cust_string6_de_DE | string | - | 256 | Yes |
| cust_string6_defaultValue | string | - | 256 | Yes |
| cust_string6_en_GB | string | - | 256 | Yes |
| cust_string6_en_US | string | - | 256 | Yes |
| cust_string6_es_ES | string | - | 256 | Yes |
| cust_string6_fr_FR | string | - | 256 | Yes |
| cust_string6_ja_JP | string | - | 256 | Yes |
| cust_string6_ko_KR | string | - | 256 | Yes |
| cust_string6_localized | string | - | 256 | Yes |
| cust_string6_nl_NL | string | - | 256 | Yes |
| cust_string6_pt_BR | string | - | 256 | Yes |
| cust_string6_pt_PT | string | - | 256 | Yes |
| cust_string6_ru_RU | string | - | 256 | Yes |
| cust_string6_zh_CN | string | - | 256 | Yes |
| cust_string6_zh_TW | string | - | 256 | Yes |
| cust_string7_de_DE | string | - | 256 | Yes |
| cust_string7_defaultValue | string | - | 256 | Yes |
| cust_string7_en_GB | string | - | 256 | Yes |
| cust_string7_en_US | string | - | 256 | Yes |
| cust_string7_es_ES | string | - | 256 | Yes |
| cust_string7_fr_FR | string | - | 256 | Yes |
| cust_string7_ja_JP | string | - | 256 | Yes |
| cust_string7_ko_KR | string | - | 256 | Yes |
| cust_string7_localized | string | - | 256 | Yes |
| cust_string7_nl_NL | string | - | 256 | Yes |
| cust_string7_pt_BR | string | - | 256 | Yes |
| cust_string7_pt_PT | string | - | 256 | Yes |
| cust_string7_ru_RU | string | - | 256 | Yes |
| cust_string7_zh_CN | string | - | 256 | Yes |
| cust_string7_zh_TW | string | - | 256 | Yes |
| cust_string8_de_DE | string | - | 256 | Yes |
| cust_string8_defaultValue | string | - | 256 | Yes |
| cust_string8_en_GB | string | - | 256 | Yes |
| cust_string8_en_US | string | - | 256 | Yes |
| cust_string8_es_ES | string | - | 256 | Yes |
| cust_string8_fr_FR | string | - | 256 | Yes |
| cust_string8_ja_JP | string | - | 256 | Yes |
| cust_string8_ko_KR | string | - | 256 | Yes |
| cust_string8_localized | string | - | 256 | Yes |
| cust_string8_nl_NL | string | - | 256 | Yes |
| cust_string8_pt_BR | string | - | 256 | Yes |
| cust_string8_pt_PT | string | - | 256 | Yes |
| cust_string8_ru_RU | string | - | 256 | Yes |
| cust_string8_zh_CN | string | - | 256 | Yes |
| cust_string8_zh_TW | string | - | 256 | Yes |
| cust_string9_de_DE | string | - | 256 | Yes |
| cust_string9_defaultValue | string | - | 256 | Yes |
| cust_string9_en_GB | string | - | 256 | Yes |
| cust_string9_en_US | string | - | 256 | Yes |
| cust_string9_es_ES | string | - | 256 | Yes |
| cust_string9_fr_FR | string | - | 256 | Yes |
| cust_string9_ja_JP | string | - | 256 | Yes |
| cust_string9_ko_KR | string | - | 256 | Yes |
| cust_string9_localized | string | - | 256 | Yes |
| cust_string9_nl_NL | string | - | 256 | Yes |
| cust_string9_pt_BR | string | - | 256 | Yes |
| cust_string9_pt_PT | string | - | 256 | Yes |
| cust_string9_ru_RU | string | - | 256 | Yes |
| cust_string9_zh_CN | string | - | 256 | Yes |
| cust_string9_zh_TW | string | - | 256 | Yes |
| customString1 | string | - | 70 | Yes |
| customString1TranslationNav | Object | - | - | Yes |
| customString2 | string | - | 32 | Yes |
| customString2TranslationNav | Object | - | - | Yes |
| customString3 | string | - | 128 | Yes |
| customString4 | string | - | 128 | Yes |
| customString5 | string | - | 128 | Yes |
| customString6 | string | - | 256 | Yes |
| customString6TranslationNav | Object | - | - | Yes |
| customString7 | string | - | 256 | Yes |
| customString7TranslationNav | Object | - | - | Yes |
| customString8 | string | - | 256 | Yes |
| customString8TranslationNav | Object | - | - | Yes |
| customString9 | string | - | 256 | Yes |
| customString9TranslationNav | Object | - | - | Yes |
| defaultEmployeeClass | string | - | 128 | Yes |
| defaultJobLevel | string | - | 128 | Yes |
| defaultSupervisorLevel | string | - | 128 | Yes |
| description | string | - | 4000 | Yes |
| descriptionTranslationNav | Object | - | - | Yes |
| description_de_DE | string | - | 4000 | Yes |
| description_defaultValue | string | - | 4000 | Yes |
| description_en_GB | string | - | 4000 | Yes |
| description_en_US | string | - | 4000 | Yes |
| description_es_ES | string | - | 4000 | Yes |
| description_fr_FR | string | - | 4000 | Yes |
| description_ja_JP | string | - | 4000 | Yes |
| description_ko_KR | string | - | 4000 | Yes |
| description_localized | string | - | 4000 | Yes |
| description_nl_NL | string | - | 4000 | Yes |
| description_pt_BR | string | - | 4000 | Yes |
| description_pt_PT | string | - | 4000 | Yes |
| description_ru_RU | string | - | 4000 | Yes |
| description_zh_CN | string | - | 4000 | Yes |
| description_zh_TW | string | - | 4000 | Yes |
| employeeClass | string | - | 128 | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| grade | string | - | 32 | Yes |
| gradeNav | Ref(FOPayGrade) | - | - | No |
| isFulltimeEmployee | boolean | - | - | Yes |
| isRegular | string | - | 128 | Yes |
| jobFunction | string | - | 128 | Yes |
| jobFunctionNav | Ref(FOJobFunction) | - | - | No |
| jobLevel | string | - | 128 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| localNavAUS | Ref(FOJobClassLocalAUS) | - | - | No |
| localNavBRA | Ref(FOJobClassLocalBRA) | - | - | No |
| localNavCAN | Ref(FOJobClassLocalCAN) | - | - | No |
| localNavDEFLT | Ref(FOJobClassLocalDEFLT) | - | - | No |
| localNavFRA | Ref(FOJobClassLocalFRA) | - | - | No |
| localNavGBR | Ref(FOJobClassLocalGBR) | - | - | No |
| localNavITA | Ref(FOJobClassLocalITA) | - | - | No |
| localNavUSA | Ref(FOJobClassLocalUSA) | - | - | No |
| name | string | - | 32 | Yes |
| nameTranslationNav | Object | - | - | Yes |
| name_de_DE | string | - | 32 | Yes |
| name_defaultValue | string | - | 32 | Yes |
| name_en_GB | string | - | 32 | Yes |
| name_en_US | string | - | 32 | Yes |
| name_es_ES | string | - | 32 | Yes |
| name_fr_FR | string | - | 32 | Yes |
| name_ja_JP | string | - | 32 | Yes |
| name_ko_KR | string | - | 32 | Yes |
| name_localized | string | - | 32 | Yes |
| name_nl_NL | string | - | 32 | Yes |
| name_pt_BR | string | - | 32 | Yes |
| name_pt_PT | string | - | 32 | Yes |
| name_ru_RU | string | - | 32 | Yes |
| name_zh_CN | string | - | 32 | Yes |
| name_zh_TW | string | - | 32 | Yes |
| parentJobCode | string | - | 128 | Yes |
| parentJobCodeNav | Ref(FOJobCode) | - | - | No |
| regularTemporary | string | - | 128 | Yes |
| standardHours | number | double | - | Yes |
| startDate | string | - | - | No |
| status | string | - | 255 | Yes |
| supervisorLevel | string | - | 128 | Yes |
| toJobClassificationCountry | Object | - | - | Yes |
| workerCompCode | string | - | 32 | Yes |

### FOJobFunction

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| description | string | - | 128 | Yes |
| descriptionTranslationNav | Ref(FoTranslation) | - | - | No |
| description_de_DE | string | - | 128 | Yes |
| description_defaultValue | string | - | 128 | Yes |
| description_en_GB | string | - | 128 | Yes |
| description_en_US | string | - | 128 | Yes |
| description_es_ES | string | - | 128 | Yes |
| description_fr_FR | string | - | 128 | Yes |
| description_ja_JP | string | - | 128 | Yes |
| description_ko_KR | string | - | 128 | Yes |
| description_localized | string | - | 128 | Yes |
| description_nl_NL | string | - | 128 | Yes |
| description_pt_BR | string | - | 128 | Yes |
| description_pt_PT | string | - | 128 | Yes |
| description_ru_RU | string | - | 128 | Yes |
| description_zh_CN | string | - | 128 | Yes |
| description_zh_TW | string | - | 128 | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| jobFunctionType | string | - | 128 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| name | string | - | 32 | Yes |
| nameTranslationNav | Ref(FoTranslation) | - | - | No |
| name_de_DE | string | - | 32 | Yes |
| name_defaultValue | string | - | 32 | Yes |
| name_en_GB | string | - | 32 | Yes |
| name_en_US | string | - | 32 | Yes |
| name_es_ES | string | - | 32 | Yes |
| name_fr_FR | string | - | 32 | Yes |
| name_ja_JP | string | - | 32 | Yes |
| name_ko_KR | string | - | 32 | Yes |
| name_localized | string | - | 32 | Yes |
| name_nl_NL | string | - | 32 | Yes |
| name_pt_BR | string | - | 32 | Yes |
| name_pt_PT | string | - | 32 | Yes |
| name_ru_RU | string | - | 32 | Yes |
| name_zh_CN | string | - | 32 | Yes |
| name_zh_TW | string | - | 32 | Yes |
| parentFunctionCode | string | - | 128 | Yes |
| parentFunctionCodeNav | Ref(FOJobFunction) | - | - | No |
| startDate | string | - | - | No |
| status | string | - | 255 | Yes |
| type | string | - | 128 | Yes |

### FOLegalEntityLocalARG

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| country | string | - | 128 | No |
| countryNav | Ref(Territory) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| cuit | string | - | 11 | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| startDate | string | - | - | No |
| status | string | - | 255 | Yes |

### FOLegalEntityLocalDEFLT

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| country | string | - | - | No |
| countryNav | Ref(Territory) | - | - | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| startDate | string | - | - | No |
| status | string | - | - | Yes |

### FOLegalEntityLocalDEU

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| country | string | - | 128 | No |
| countryNav | Ref(Territory) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| genericString1 | string | - | 45 | Yes |
| genericString2 | string | - | 45 | Yes |
| genericString3 | string | - | 45 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| startDate | string | - | - | No |
| status | string | - | 255 | Yes |

### FOLegalEntityLocalESP

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| country | string | - | 128 | No |
| countryNav | Ref(Territory) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| genericString1 | string | - | 45 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| startDate | string | - | - | No |
| status | string | - | 255 | Yes |

### FOLegalEntityLocalFRA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| country | string | - | 128 | No |
| countryNav | Ref(Territory) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| genericNumber1 | string | int64 | - | Yes |
| genericNumber2 | string | int64 | - | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| nafCodePost2008 | string | - | 45 | Yes |
| startDate | string | - | - | No |
| status | string | - | 255 | Yes |

### FOLegalEntityLocalUSA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| country | string | - | 128 | No |
| countryNav | Ref(Territory) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| genericNumber1 | string | int64 | - | Yes |
| genericString1 | string | - | 45 | Yes |
| genericString2 | string | - | 45 | Yes |
| genericString3 | string | - | 45 | Yes |
| genericString4 | string | - | 45 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| legalEntityType | string | - | 128 | Yes |
| startDate | string | - | - | No |
| status | string | - | 255 | Yes |

### FOLocation

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| addressAddress1 | string | - | 256 | Yes |
| addressAddress2 | string | - | 256 | Yes |
| addressAddress3 | string | - | 256 | Yes |
| addressAddress4 | string | - | 256 | Yes |
| addressAddress5 | string | - | 256 | Yes |
| addressCity | string | - | 256 | Yes |
| addressCountry | string | - | 256 | Yes |
| addressCounty | string | - | 256 | Yes |
| addressNavDEFLT | Ref(FOCorporateAddressDEFLT) | - | - | No |
| addressProvince | string | - | 256 | Yes |
| addressState | string | - | 256 | Yes |
| addressZipCode | string | - | 256 | Yes |
| companyFlx | string | - | - | Yes |
| companyFlxNav | Object | - | - | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| description | string | - | 128 | Yes |
| descriptionTranslationNav | Ref(FoTranslation) | - | - | No |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| geozoneFlx | string | - | 32 | Yes |
| geozoneFlxNav | Ref(FOGeozone) | - | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| locationGroup | string | - | - | Yes |
| locationGroupNav | Ref(FOLocationGroup) | - | - | No |
| name | string | - | 32 | Yes |
| nameTranslationNav | Ref(FoTranslation) | - | - | No |
| standardHours | number | double | - | Yes |
| startDate | string | - | - | No |
| status | string | - | - | Yes |
| timezone | string | - | - | Yes |

### FOLocationGroup

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| description | string | - | 128 | Yes |
| descriptionTranslationNav | Ref(FoTranslation) | - | - | No |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| name | string | - | 32 | Yes |
| nameTranslationNav | Ref(FoTranslation) | - | - | No |
| startDate | string | - | - | No |
| status | string | - | - | Yes |

### FOPayComponent

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| basePayComponentGroup | string | - | - | Yes |
| basePayComponentGroupNav | Ref(FOPayComponentGroup) | - | - | No |
| canOverride | boolean | - | - | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| currency | string | - | - | Yes |
| description | string | - | 128 | Yes |
| descriptionTranslationNav | Ref(FoTranslation) | - | - | No |
| displayOnSelfService | boolean | - | - | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| frequencyCode | string | - | - | Yes |
| frequencyCodeNav | Ref(FOFrequency) | - | - | No |
| isEarning | boolean | - | - | Yes |
| isEndDatedPayment | boolean | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| maxFractionDigits | string | int64 | - | Yes |
| name | string | - | 32 | Yes |
| nameTranslationNav | Ref(FoTranslation) | - | - | No |
| payComponentType | string | - | 32 | Yes |
| payComponentValue | number | double | - | Yes |
| recurring | boolean | - | - | Yes |
| selfServiceDescription | string | - | 32 | Yes |
| startDate | string | - | - | No |
| status | string | - | - | Yes |
| target | boolean | - | - | Yes |
| taxTreatment | string | - | 32 | Yes |
| usedForCompPlanning | string | - | 32 | Yes |

### FOPayComponentGroup

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| currency | string | - | - | Yes |
| description | string | - | 128 | Yes |
| descriptionTranslationNav | Ref(FoTranslation) | - | - | No |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| name | string | - | 32 | Yes |
| nameTranslationNav | Ref(FoTranslation) | - | - | No |
| payComponentFlx | string | - | - | Yes |
| payComponentFlxNav | Object | - | - | Yes |
| showOnCompUI | boolean | - | - | Yes |
| sortOrder | number | double | - | Yes |
| startDate | string | - | - | No |
| status | string | - | - | Yes |
| useForComparatioCalc | boolean | - | - | Yes |
| useForRangePenetration | boolean | - | - | Yes |

### FOPayGrade

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| customString1 | string | - | 256 | Yes |
| description | string | - | 128 | Yes |
| descriptionTranslationNav | Ref(FoTranslation) | - | - | No |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| name | string | - | 32 | Yes |
| nameTranslationNav | Ref(FoTranslation) | - | - | No |
| paygradeLevel | string | int64 | - | Yes |
| startDate | string | - | - | No |
| status | string | - | - | Yes |

### FOPayGroup

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| dataDelimiter | string | - | 32 | Yes |
| decimalPoint | string | - | 32 | Yes |
| description | string | - | 128 | Yes |
| descriptionTranslationNav | Ref(FoTranslation) | - | - | No |
| description_de_DE | string | - | 128 | Yes |
| description_defaultValue | string | - | 128 | Yes |
| description_en_GB | string | - | 128 | Yes |
| description_en_US | string | - | 128 | Yes |
| description_es_ES | string | - | 128 | Yes |
| description_fr_FR | string | - | 128 | Yes |
| description_ja_JP | string | - | 128 | Yes |
| description_ko_KR | string | - | 128 | Yes |
| description_localized | string | - | 128 | Yes |
| description_nl_NL | string | - | 128 | Yes |
| description_pt_BR | string | - | 128 | Yes |
| description_pt_PT | string | - | 128 | Yes |
| description_ru_RU | string | - | 128 | Yes |
| description_zh_CN | string | - | 128 | Yes |
| description_zh_TW | string | - | 128 | Yes |
| earliestChangeDate | string | - | - | Yes |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| lag | string | int64 | - | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| name | string | - | 32 | Yes |
| nameTranslationNav | Ref(FoTranslation) | - | - | No |
| name_de_DE | string | - | 32 | Yes |
| name_defaultValue | string | - | 32 | Yes |
| name_en_GB | string | - | 32 | Yes |
| name_en_US | string | - | 32 | Yes |
| name_es_ES | string | - | 32 | Yes |
| name_fr_FR | string | - | 32 | Yes |
| name_ja_JP | string | - | 32 | Yes |
| name_ko_KR | string | - | 32 | Yes |
| name_localized | string | - | 32 | Yes |
| name_nl_NL | string | - | 32 | Yes |
| name_pt_BR | string | - | 32 | Yes |
| name_pt_PT | string | - | 32 | Yes |
| name_ru_RU | string | - | 32 | Yes |
| name_zh_CN | string | - | 32 | Yes |
| name_zh_TW | string | - | 32 | Yes |
| payFrequency | string | - | 128 | Yes |
| paymentFrequency | string | - | 128 | Yes |
| payrollVendorId | string | - | 32 | Yes |
| primaryContactEmail | string | - | 256 | Yes |
| primaryContactID | string | - | 256 | Yes |
| primaryContactName | string | - | 256 | Yes |
| secondaryContactEmail | string | - | 256 | Yes |
| secondaryContactID | string | - | 256 | Yes |
| secondaryContactName | string | - | 256 | Yes |
| startDate | string | - | - | No |
| status | string | - | 255 | Yes |
| weeksInPayPeriod | string | int64 | - | Yes |

### FOPayRange

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| companyFlx | string | - | 32 | Yes |
| companyFlxNav | Ref(FOCompany) | - | - | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| currency | string | - | 45 | Yes |
| description | string | - | 128 | Yes |
| descriptionTranslationNav | Ref(FoTranslation) | - | - | No |
| endDate | string | - | - | Yes |
| externalCode | string | - | 32 | No |
| frequencyCode | string | - | - | Yes |
| frequencyCodeNav | Ref(FOFrequency) | - | - | No |
| geozoneFlx | string | - | 32 | Yes |
| geozoneFlxNav | Ref(FOGeozone) | - | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| maximumPay | string | decimal | - | Yes |
| midPoint | string | decimal | - | Yes |
| minimumPay | string | decimal | - | Yes |
| name | string | - | 32 | Yes |
| nameTranslationNav | Ref(FoTranslation) | - | - | No |
| payGradeFlx | string | - | 32 | Yes |
| payGradeFlxNav | Ref(FOPayGrade) | - | - | No |
| startDate | string | - | - | No |
| status | string | - | - | Yes |

### FOWfConfig

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| actionType | string | - | - | Yes |
| approverRole | string | - | - | Yes |
| approverType | string | - | 32 | Yes |
| context | string | - | 32 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| description | string | - | 128 | Yes |
| descriptionTranslationNav | Ref(FoTranslation) | - | - | No |
| emailConfiguration | string | decimal | - | Yes |
| escalation | string | - | 128 | Yes |
| externalCode | string | - | 32 | No |
| futureDatedAlternateWorkflow | string | - | - | Yes |
| futureDatedAlternateWorkflowNav | Ref(FOWfConfig) | - | - | No |
| isCcLinkToApprovalPage | boolean | - | - | Yes |
| isDelegateSupported | boolean | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| name | string | - | 32 | Yes |
| nameTranslationNav | Ref(FoTranslation) | - | - | No |
| relationshipToApprover | string | - | - | Yes |
| remindIndays | string | int64 | - | Yes |
| respectRBP | boolean | - | - | Yes |
| skipType | string | - | - | Yes |
| stepNum | string | int64 | - | Yes |
| wfStepApproverNav | Object | - | - | Yes |

### FOWfConfigStepApprover

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| actionType | string | - | - | Yes |
| approverDynamicRoleNav | Object | - | - | Yes |
| approverPositionRelationship | string | - | - | Yes |
| approverRole | string | - | - | Yes |
| approverType | string | - | 32 | Yes |
| context | string | - | 32 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| emailConfiguration | string | - | 128 | Yes |
| externalCode | string | - | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| relationshipToApprover | string | - | - | Yes |
| relationshipToPosition | string | - | - | Yes |
| respectRBP | boolean | - | - | Yes |
| skipType | string | - | - | Yes |
| stepNum | string | int64 | - | No |

### FoTranslation

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| foField | string | - | 255 | Yes |
| foObjectID | string | - | 128 | Yes |
| foType | string | - | 255 | Yes |
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
| value_de_DE | string | - | 255 | Yes |
| value_defaultValue | string | - | 255 | Yes |
| value_en_GB | string | - | 255 | Yes |
| value_en_US | string | - | 255 | Yes |
| value_es_ES | string | - | 255 | Yes |
| value_fr_FR | string | - | 255 | Yes |
| value_ja_JP | string | - | 255 | Yes |
| value_ko_KR | string | - | 255 | Yes |
| value_localized | string | - | 255 | Yes |
| value_nl_NL | string | - | 255 | Yes |
| value_pt_BR | string | - | 255 | Yes |
| value_pt_PT | string | - | 255 | Yes |
| value_ru_RU | string | - | 255 | Yes |
| value_zh_CN | string | - | 255 | Yes |
| value_zh_TW | string | - | 255 | Yes |

### JobClassificationAUS

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| JobClassificationCountry_country | string | - | 128 | No |
| JobClassification_effectiveStartDate | string | - | - | No |
| JobClassification_externalCode | string | - | 32 | No |
| ascoCode | string | - | 32 | Yes |
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

### JobClassificationBRA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| JobClassificationCountry_country | string | - | 128 | No |
| JobClassification_effectiveStartDate | string | - | - | No |
| JobClassification_externalCode | string | - | 32 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| cust_occupationalCode | string | - | 32 | Yes |
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
| occupationalCode | string | - | 128 | Yes |

### JobClassificationCAN

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| JobClassificationCountry_country | string | - | 128 | No |
| JobClassification_effectiveStartDate | string | - | - | No |
| JobClassification_externalCode | string | - | 32 | No |
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
| occupationalClassification | string | - | 255 | Yes |

### JobClassificationCountry

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| JobClassification_effectiveStartDate | string | - | - | No |
| JobClassification_externalCode | string | - | 32 | No |
| country | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveStatus | string | - | 255 | Yes |
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
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| toJobClassificationAUS | Ref(JobClassificationAUS) | - | - | No |
| toJobClassificationBRA | Ref(JobClassificationBRA) | - | - | No |
| toJobClassificationCAN | Ref(JobClassificationCAN) | - | - | No |
| toJobClassificationFRA | Ref(JobClassificationFRA) | - | - | No |
| toJobClassificationGBR | Ref(JobClassificationGBR) | - | - | No |
| toJobClassificationITA | Ref(JobClassificationITA) | - | - | No |
| toJobClassificationUSA | Ref(JobClassificationUSA) | - | - | No |
| toJobClassificationZAF | Ref(JobClassificationZAF) | - | - | No |

### JobClassificationFRA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| JobClassificationCountry_country | string | - | 128 | No |
| JobClassification_effectiveStartDate | string | - | - | No |
| JobClassification_externalCode | string | - | 32 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| employeeCategory | string | int64 | - | Yes |
| externalCode | string | int64 | - | No |
| inseeCode | string | - | 45 | Yes |
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

### JobClassificationGBR

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| JobClassificationCountry_country | string | - | 128 | No |
| JobClassification_effectiveStartDate | string | - | - | No |
| JobClassification_externalCode | string | - | 32 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| cust_long1 | string | - | 128 | Yes |
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
| occupationalCode | string | int64 | - | Yes |

### JobClassificationITA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| JobClassificationCountry_country | string | - | 128 | No |
| JobClassification_effectiveStartDate | string | - | - | No |
| JobClassification_externalCode | string | - | 32 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| inailCode | string | int64 | - | Yes |
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

### JobClassificationUSA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| JobClassificationCountry_country | string | - | 128 | No |
| JobClassification_effectiveStartDate | string | - | - | No |
| JobClassification_externalCode | string | - | 32 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| eeo1JobCategory | string | - | 128 | Yes |
| eeo4JobCategory | string | - | 128 | Yes |
| eeo5JobCategory | string | - | 128 | Yes |
| eeo6JobCategory | string | - | 128 | Yes |
| eeoJobGroup | string | - | 128 | Yes |
| externalCode | string | int64 | - | No |
| flsaStatusUSA | string | - | 128 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| localJobTitle | string | - | 32 | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |

### JobClassificationZAF

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| JobClassificationCountry_country | string | - | 128 | No |
| JobClassification_effectiveStartDate | string | - | - | No |
| JobClassification_externalCode | string | - | 32 | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| occupationalLevel | string | - | 128 | Yes |

### LegalEntityARG

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| LegalEntity_effectiveStartDate | string | - | - | No |
| LegalEntity_externalCode | string | - | 32 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| cuit | string | - | 11 | Yes |
| cuitCode | string | int64 | - | Yes |
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

### LegalEntityBLR

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| LegalEntity_effectiveStartDate | string | - | - | No |
| LegalEntity_externalCode | string | - | 32 | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| unpNumber | string | - | 9 | Yes |

### LegalEntityBOL

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| LegalEntity_effectiveStartDate | string | - | - | No |
| LegalEntity_externalCode | string | - | 32 | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| employerSocialSecurityNumber | string | - | 13 | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| taxNumber | string | - | 12 | Yes |

### LegalEntityCAN

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| LegalEntity_effectiveStartDate | string | - | - | No |
| LegalEntity_externalCode | string | - | 32 | No |
| businessNumber | string | - | 9 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| neqNumber | string | - | 10 | Yes |

### LegalEntityDEU

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| LegalEntity_effectiveStartDate | string | - | - | No |
| LegalEntity_externalCode | string | - | 32 | No |
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
| socialAccidentInsurance | string | - | 45 | Yes |
| socialAccidentInsuranceRegistrationNumber | string | - | 45 | Yes |
| taxUnit | string | - | 45 | Yes |

### LegalEntityESP

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| LegalEntity_effectiveStartDate | string | - | - | No |
| LegalEntity_externalCode | string | - | 32 | No |
| certificadoDeIdentificacionFiscal | string | - | 45 | Yes |
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

### LegalEntityFRA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| LegalEntity_effectiveStartDate | string | - | - | No |
| LegalEntity_externalCode | string | - | 32 | No |
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
| nafCode | string | int64 | - | Yes |
| nafCodePost2008 | string | - | 45 | Yes |
| sirenCode | string | int64 | - | Yes |

### LegalEntityPRY

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| LegalEntity_effectiveStartDate | string | - | - | No |
| LegalEntity_externalCode | string | - | 32 | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| socialSecurity | string | - | 13 | Yes |
| taxNumber | string | - | 13 | Yes |

### LegalEntityRUS

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| LegalEntity_effectiveStartDate | string | - | - | No |
| LegalEntity_externalCode | string | - | 32 | No |
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
| okpoNumber | string | - | 10 | Yes |

### LegalEntitySAU

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| LegalEntity_effectiveStartDate | string | - | - | No |
| LegalEntity_externalCode | string | - | 32 | No |
| companyCommercialRegistrationNumber | string | - | 255 | Yes |
| companyRepresentative | string | - | 100 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| gosiOfficeBranch | string | - | 255 | Yes |
| gosiRegistrationNumber | string | - | 9 | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### LegalEntitySGP

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| LegalEntity_effectiveStartDate | string | - | - | No |
| LegalEntity_externalCode | string | - | 32 | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| uniqueEntityNumber | string | - | 255 | Yes |

### LegalEntitySVN

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| LegalEntity_effectiveStartDate | string | - | - | No |
| LegalEntity_externalCode | string | - | 32 | No |
| companyMasterNumber | string | - | 10 | Yes |
| companyRegistrationNumber | string | int64 | - | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| employerTaxNumber | string | int64 | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### LegalEntityTHA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| LegalEntity_effectiveStartDate | string | - | - | No |
| LegalEntity_externalCode | string | - | 32 | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| socialSecurityNumber | string | - | 10 | Yes |

### LegalEntityTUN

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| LegalEntity_effectiveStartDate | string | - | - | No |
| LegalEntity_externalCode | string | - | 32 | No |
| categoryCode | string | - | 1 | Yes |
| codeVat | string | - | 1 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| fiscalID | string | - | 9 | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| numberOfSecondaryEstablishment | string | - | 3 | Yes |

### LegalEntityUSA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| LegalEntity_effectiveStartDate | string | - | - | No |
| LegalEntity_externalCode | string | - | 32 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| eeoCompanyCode | string | - | 45 | Yes |
| employerID | string | - | 45 | Yes |
| externalCode | string | int64 | - | No |
| fedReserveBankDistrict | string | - | 45 | Yes |
| federalReserveBankID | string | - | 45 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| legalEntityType | string | - | 128 | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |

### LocalizedData

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| localizedDataCode | string | - | 38 | No |
| localizedDataLocale | string | - | 32 | No |
| localizedDataTranslation | string | - | 4000 | Yes |

### PayCalendar

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
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
| payGroup | string | - | 128 | No |
| payGroupNav | Object | - | - | Yes |
| toPayPeriod | Object | - | - | Yes |

### PayPeriod

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PayCalendar_payGroup | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| cust_payPeriodsPerYear | string | int64 | - | Yes |
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
| offcycle | boolean | - | - | Yes |
| payCheckIssueDate | string | - | - | Yes |
| payPeriodBeginDate | string | - | - | Yes |
| payPeriodEndDate | string | - | - | Yes |
| processingRunId | string | - | 256 | Yes |
| runType | string | - | 128 | Yes |

### PayScaleArea

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| code | string | - | 128 | No |
| country | string | - | 128 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| externalName_de_DE | string | - | 128 | Yes |
| externalName_defaultValue | string | - | 128 | Yes |
| externalName_en_GB | string | - | 128 | Yes |
| externalName_en_US | string | - | 128 | Yes |
| externalName_es_ES | string | - | 128 | Yes |
| externalName_fr_FR | string | - | 128 | Yes |
| externalName_ja_JP | string | - | 128 | Yes |
| externalName_ko_KR | string | - | 128 | Yes |
| externalName_localized | string | - | 128 | Yes |
| externalName_nl_NL | string | - | 128 | Yes |
| externalName_pt_BR | string | - | 128 | Yes |
| externalName_pt_PT | string | - | 128 | Yes |
| externalName_ru_RU | string | - | 128 | Yes |
| externalName_zh_CN | string | - | 128 | Yes |
| externalName_zh_TW | string | - | 128 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemCreatedBy | string | - | 255 | Yes |
| mdfSystemCreatedDate | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemLastModifiedBy | string | - | 255 | Yes |
| mdfSystemLastModifiedDate | string | - | - | Yes |
| mdfSystemLastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| payScaleArea | string | - | 128 | Yes |

### PayScaleGroup

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| code | string | - | 128 | No |
| country | string | - | 128 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
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
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemCreatedBy | string | - | 255 | Yes |
| mdfSystemCreatedDate | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemLastModifiedBy | string | - | 255 | Yes |
| mdfSystemLastModifiedDate | string | - | - | Yes |
| mdfSystemLastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| payScaleArea | string | - | 128 | Yes |
| payScaleAreaNav | Ref(PayScaleArea) | - | - | No |
| payScaleGroup | string | - | 255 | Yes |
| payScaleType | string | - | 128 | Yes |
| payScaleTypeNav | Ref(PayScaleType) | - | - | No |

### PayScaleLevel

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| code | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
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
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemCreatedBy | string | - | 255 | Yes |
| mdfSystemCreatedDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemLastModifiedBy | string | - | 255 | Yes |
| mdfSystemLastModifiedDate | string | - | - | Yes |
| mdfSystemLastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| nextPayScaleLevel | string | - | 128 | Yes |
| nextPayScaleLevelNav | Ref(PayScaleLevel) | - | - | No |
| payScaleGroup | Ref(PayScaleGroup) | - | - | No |
| payScaleLevel | string | - | 128 | Yes |
| toPayScalePayComponents | Object | - | - | Yes |

### PayScalePayComponent

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PayScaleLevel_code | string | - | 128 | No |
| PayScaleLevel_effectiveStartDate | string | - | - | No |
| amount | string | decimal | - | Yes |
| code | string | - | 32 | No |
| codeNav | Object | - | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| currency | string | - | 128 | Yes |
| frequency | string | - | 32 | Yes |
| frequencyNav | Ref(FOFrequency) | - | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemCreatedBy | string | - | 255 | Yes |
| mdfSystemCreatedDate | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemLastModifiedBy | string | - | 255 | Yes |
| mdfSystemLastModifiedDate | string | - | - | Yes |
| mdfSystemLastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| number | string | decimal | - | Yes |
| percentage | string | decimal | - | Yes |
| rate | string | decimal | - | Yes |
| unit | string | - | 128 | Yes |

### PayScaleType

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| code | string | - | 128 | No |
| country | string | - | 128 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| externalName_de_DE | string | - | 128 | Yes |
| externalName_defaultValue | string | - | 128 | Yes |
| externalName_en_GB | string | - | 128 | Yes |
| externalName_en_US | string | - | 128 | Yes |
| externalName_es_ES | string | - | 128 | Yes |
| externalName_fr_FR | string | - | 128 | Yes |
| externalName_ja_JP | string | - | 128 | Yes |
| externalName_ko_KR | string | - | 128 | Yes |
| externalName_localized | string | - | 128 | Yes |
| externalName_nl_NL | string | - | 128 | Yes |
| externalName_pt_BR | string | - | 128 | Yes |
| externalName_pt_PT | string | - | 128 | Yes |
| externalName_ru_RU | string | - | 128 | Yes |
| externalName_zh_CN | string | - | 128 | Yes |
| externalName_zh_TW | string | - | 128 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemCreatedBy | string | - | 255 | Yes |
| mdfSystemCreatedDate | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemLastModifiedBy | string | - | 255 | Yes |
| mdfSystemLastModifiedDate | string | - | - | Yes |
| mdfSystemLastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| payScaleType | string | - | 128 | Yes |

### Periods

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| FiscalYearVariant_externalCode | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| day | string | - | 255 | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| month | string | - | 255 | Yes |
| period | string | - | 255 | Yes |
| yearShift | string | - | 255 | Yes |

### Territory

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| territoryCode | string | - | 32 | No |
| territoryName | string | - | 512 | Yes |

## Object Primary Keys

Primary key fields identified for each entity:

- **BudgetGroup**: externalCode, mdfSystemRecordId, userId, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **FOBusinessUnit**: externalCode
- **FOCompany**: externalCode
- **FOCorporateAddressDEFLT**: addressId
- **FOCostCenter**: externalCode
- **FODepartment**: externalCode
- **FODivision**: externalCode
- **FODynamicRole**: dynamicRoleAssignmentId, externalCode
- **FOEventReason**: externalCode
- **FOFrequency**: externalCode
- **FOGeozone**: externalCode
- **FOJobClassLocalAUS**: externalCode
- **FOJobClassLocalBRA**: externalCode
- **FOJobClassLocalCAN**: externalCode
- **FOJobClassLocalDEFLT**: externalCode
- **FOJobClassLocalFRA**: externalCode
- **FOJobClassLocalGBR**: externalCode
- **FOJobClassLocalITA**: externalCode
- **FOJobClassLocalUSA**: externalCode
- **FOJobCode**: externalCode
- **FOJobFunction**: externalCode
- **FOLegalEntityLocalARG**: externalCode
- **FOLegalEntityLocalDEFLT**: externalCode
- **FOLegalEntityLocalDEU**: externalCode
- **FOLegalEntityLocalESP**: externalCode
- **FOLegalEntityLocalFRA**: externalCode
- **FOLegalEntityLocalUSA**: externalCode
- **FOLocation**: externalCode
- **FOLocationGroup**: externalCode
- **FOPayComponent**: externalCode
- **FOPayComponentGroup**: externalCode
- **FOPayGrade**: externalCode
- **FOPayGroup**: externalCode, payrollVendorId
- **FOPayRange**: externalCode
- **FOWfConfig**: externalCode
- **FOWfConfigStepApprover**: externalCode
- **FoTranslation**: mdfSystemEntityId, externalCode, mdfSystemVersionId, mdfSystemRecordId
- **JobClassificationAUS**: externalCode, JobClassification_effectiveStartDate, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, JobClassification_externalCode
- **JobClassificationBRA**: externalCode, JobClassification_effectiveStartDate, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, JobClassification_externalCode
- **JobClassificationCAN**: externalCode, JobClassification_effectiveStartDate, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, JobClassification_externalCode
- **JobClassificationCountry**: mdfSystemRecordId, JobClassification_effectiveStartDate, mdfSystemEntityId, mdfSystemVersionId, JobClassification_externalCode
- **JobClassificationFRA**: externalCode, JobClassification_effectiveStartDate, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, JobClassification_externalCode
- **JobClassificationGBR**: externalCode, JobClassification_effectiveStartDate, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, JobClassification_externalCode
- **JobClassificationITA**: externalCode, JobClassification_effectiveStartDate, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, JobClassification_externalCode
- **JobClassificationUSA**: externalCode, JobClassification_effectiveStartDate, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, JobClassification_externalCode
- **JobClassificationZAF**: JobClassification_externalCode, externalCode, JobClassification_effectiveStartDate
- **LegalEntityARG**: LegalEntity_effectiveStartDate, externalCode, mdfSystemRecordId, LegalEntity_externalCode, mdfSystemEntityId, mdfSystemVersionId
- **LegalEntityBLR**: LegalEntity_effectiveStartDate, externalCode, LegalEntity_externalCode
- **LegalEntityBOL**: LegalEntity_effectiveStartDate, externalCode, LegalEntity_externalCode
- **LegalEntityCAN**: LegalEntity_effectiveStartDate, externalCode, LegalEntity_externalCode
- **LegalEntityDEU**: LegalEntity_effectiveStartDate, externalCode, mdfSystemRecordId, LegalEntity_externalCode, mdfSystemEntityId, mdfSystemVersionId
- **LegalEntityESP**: LegalEntity_effectiveStartDate, externalCode, mdfSystemRecordId, LegalEntity_externalCode, mdfSystemEntityId, mdfSystemVersionId
- **LegalEntityFRA**: LegalEntity_effectiveStartDate, externalCode, mdfSystemRecordId, LegalEntity_externalCode, mdfSystemEntityId, mdfSystemVersionId
- **LegalEntityPRY**: LegalEntity_effectiveStartDate, externalCode, LegalEntity_externalCode
- **LegalEntityRUS**: LegalEntity_effectiveStartDate, externalCode, mdfSystemRecordId, LegalEntity_externalCode, mdfSystemEntityId, mdfSystemVersionId
- **LegalEntitySAU**: LegalEntity_effectiveStartDate, externalCode, LegalEntity_externalCode
- **LegalEntitySGP**: LegalEntity_effectiveStartDate, externalCode, LegalEntity_externalCode
- **LegalEntitySVN**: LegalEntity_effectiveStartDate, externalCode, LegalEntity_externalCode
- **LegalEntityTHA**: LegalEntity_effectiveStartDate, externalCode, LegalEntity_externalCode
- **LegalEntityTUN**: LegalEntity_effectiveStartDate, externalCode, LegalEntity_externalCode
- **LegalEntityUSA**: LegalEntity_effectiveStartDate, externalCode, mdfSystemRecordId, LegalEntity_externalCode, mdfSystemEntityId, mdfSystemVersionId
- **LocalizedData**: externalCode
- **PayCalendar**: mdfSystemEntityId, mdfSystemRecordId, mdfSystemVersionId
- **PayPeriod**: externalCode, mdfSystemRecordId, mdfSystemEntityId, processingRunId, mdfSystemVersionId
- **PayScaleArea**: mdfSystemEntityId, mdfSystemRecordId, mdfSystemVersionId
- **PayScaleGroup**: mdfSystemEntityId, mdfSystemRecordId, mdfSystemVersionId
- **PayScaleLevel**: effectiveStartDate, mdfSystemEntityId, mdfSystemRecordId, mdfSystemVersionId
- **PayScalePayComponent**: mdfSystemEntityId, mdfSystemRecordId, mdfSystemVersionId, PayScaleLevel_effectiveStartDate
- **PayScaleType**: mdfSystemEntityId, mdfSystemRecordId, mdfSystemVersionId
- **Periods**: FiscalYearVariant_externalCode, externalCode
- **Territory**: externalCode

## Object's Ingestion Type

Ingestion types are determined based on API capabilities:


| Entity | Ingestion Type | Cursor Field |
|--------|---------------|--------------|
| BudgetGroup | cdc | lastModifiedDateTime |
| FOBusinessUnit | cdc | lastModifiedDateTime |
| FOCompany | cdc | lastModifiedDateTime |
| FOCorporateAddressDEFLT | cdc | lastModifiedDateTime |
| FOCostCenter | cdc | lastModifiedDateTime |
| FODepartment | cdc | lastModifiedDateTime |
| FODivision | cdc | lastModifiedDateTime |
| FODynamicRole | cdc | lastModifiedDateTime |
| FOEventReason | cdc | lastModifiedDateTime |
| FOFrequency | cdc | lastModifiedDateTime |
| FOGeozone | cdc | lastModifiedDateTime |
| FOJobClassLocalAUS | cdc | lastModifiedDateTime |
| FOJobClassLocalBRA | cdc | lastModifiedDateTime |
| FOJobClassLocalCAN | cdc | lastModifiedDateTime |
| FOJobClassLocalDEFLT | cdc | lastModifiedDateTime |
| FOJobClassLocalFRA | cdc | lastModifiedDateTime |
| FOJobClassLocalGBR | cdc | lastModifiedDateTime |
| FOJobClassLocalITA | cdc | lastModifiedDateTime |
| FOJobClassLocalUSA | cdc | lastModifiedDateTime |
| FOJobCode | cdc | lastModifiedDateTime |
| FOJobFunction | cdc | lastModifiedDateTime |
| FOLegalEntityLocalARG | cdc | lastModifiedDateTime |
| FOLegalEntityLocalDEFLT | cdc | lastModifiedDateTime |
| FOLegalEntityLocalDEU | cdc | lastModifiedDateTime |
| FOLegalEntityLocalESP | cdc | lastModifiedDateTime |
| FOLegalEntityLocalFRA | cdc | lastModifiedDateTime |
| FOLegalEntityLocalUSA | cdc | lastModifiedDateTime |
| FOLocation | cdc | lastModifiedDateTime |
| FOLocationGroup | cdc | lastModifiedDateTime |
| FOPayComponent | cdc | lastModifiedDateTime |
| FOPayComponentGroup | cdc | lastModifiedDateTime |
| FOPayGrade | cdc | lastModifiedDateTime |
| FOPayGroup | cdc | lastModifiedDateTime |
| FOPayRange | cdc | lastModifiedDateTime |
| FOWfConfig | cdc | lastModifiedDateTime |
| FOWfConfigStepApprover | cdc | lastModifiedDateTime |
| FoTranslation | cdc | lastModifiedDateTime |
| JobClassificationAUS | cdc | lastModifiedDateTime |
| JobClassificationBRA | cdc | lastModifiedDateTime |
| JobClassificationCAN | cdc | lastModifiedDateTime |
| JobClassificationCountry | cdc | lastModifiedDateTime |
| JobClassificationFRA | cdc | lastModifiedDateTime |
| JobClassificationGBR | cdc | lastModifiedDateTime |
| JobClassificationITA | cdc | lastModifiedDateTime |
| JobClassificationUSA | cdc | lastModifiedDateTime |
| JobClassificationZAF | cdc | lastModifiedDateTime |
| LegalEntityARG | cdc | lastModifiedDateTime |
| LegalEntityBLR | cdc | lastModifiedDateTime |
| LegalEntityBOL | cdc | lastModifiedDateTime |
| LegalEntityCAN | cdc | lastModifiedDateTime |
| LegalEntityDEU | cdc | lastModifiedDateTime |
| LegalEntityESP | cdc | lastModifiedDateTime |
| LegalEntityFRA | cdc | lastModifiedDateTime |
| LegalEntityPRY | cdc | lastModifiedDateTime |
| LegalEntityRUS | cdc | lastModifiedDateTime |
| LegalEntitySAU | cdc | lastModifiedDateTime |
| LegalEntitySGP | cdc | lastModifiedDateTime |
| LegalEntitySVN | cdc | lastModifiedDateTime |
| LegalEntityTHA | cdc | lastModifiedDateTime |
| LegalEntityTUN | cdc | lastModifiedDateTime |
| LegalEntityUSA | cdc | lastModifiedDateTime |
| LocalizedData | snapshot | - |
| PayCalendar | cdc | lastModifiedDateTime |
| PayPeriod | cdc | lastModifiedDateTime |
| PayScaleArea | cdc | lastModifiedDateTime |
| PayScaleGroup | cdc | lastModifiedDateTime |
| PayScaleLevel | cdc | lastModifiedDateTime |
| PayScalePayComponent | cdc | lastModifiedDateTime |
| PayScaleType | cdc | lastModifiedDateTime |
| Periods | cdc | lastModifiedDateTime |
| Territory | snapshot | - |

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

**Get all records from FOLegalEntityLocalUSA:**
```http
GET https://{api-server}/odata/v2/FOLegalEntityLocalUSA
Authorization: Basic {base64_credentials}
Accept: application/json
```

**Get records modified after a date (for CDC):**
```http
GET https://{api-server}/odata/v2/FOLegalEntityLocalUSA?$filter=lastModifiedDateTime ge datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc
Authorization: Basic {base64_credentials}
Accept: application/json
```

**Get specific fields with pagination:**
```http
GET https://{api-server}/odata/v2/FOLegalEntityLocalUSA?$select=externalCode,createdDateTime,lastModifiedDateTime&$top=100&$skip=0&$inlinecount=allpages
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
          "uri": "https://{api-server}/odata/v2/FOLegalEntityLocalUSA('key')",
          "type": "SFOData.FOLegalEntityLocalUSA"
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

- **SAP SuccessFactors API Spec**: `ECFoundationOrganization.json`
- **SAP Help Portal**: [Employee Central Foundation on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/6c99130f495f436e848851af76e76098.html)
- **SAP API Business Hub**: [SAP SuccessFactors APIs](https://api.sap.com/package/SuccessFactorsFoundation/overview)
- **Authentication Guide**: [SAP SuccessFactors OData API Authentication](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/5c8bca0af1654b05a83193b2922dcee2.html)
