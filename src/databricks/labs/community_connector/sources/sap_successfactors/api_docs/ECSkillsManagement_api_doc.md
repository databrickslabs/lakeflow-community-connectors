# Skills Management API Documentation

## Authorization

- **Method**: Basic Authentication
- **Header**: `Authorization: Basic base64(username:password)`
- **Username format**: `username@companyId`

## Object List

The following entities/objects are available in this API:

- BehaviorMappingEntity
- CertificationContent
- CertificationEntity
- CompetencyContent
- CompetencyEntity
- CompetencyType
- EmploymentConditionContent
- EmploymentConditionEntity
- FamilyCompetencyMappingEntity
- FamilyEntity
- FamilySkillMappingEntity
- InterviewQuestionContent
- InterviewQuestionEntity
- JDTemplateFamilyMapping
- JobCodeMappingEntity
- JobDescSection
- JobDescTemplate
- JobProfile
- JobProfileLocalizedData
- JobResponsibilityContent
- JobResponsibilityEntity
- PhysicalReqContent
- PhysicalReqEntity
- PositionCompetencyMappingEntity
- PositionEntity
- PositionSkillMappingEntity
- RatedSkillMapping
- RelevantIndustryContent
- RelevantIndustryEntity
- RoleCompetencyBehaviorMappingEntity
- RoleCompetencyMappingEntity
- RoleEntity
- RoleSkillMappingEntity
- RoleTalentPoolMappingEntity
- SelfReportSkillMapping
- SkillContent
- SkillEntity
- SkillProfile

## Object Schema

Detailed schema for each entity:

### BehaviorMappingEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| CompetencyEntity_externalCode | string | - | 128 | No |
| behaviorName_de_DE | string | - | 2048 | Yes |
| behaviorName_defaultValue | string | - | 2048 | Yes |
| behaviorName_en_GB | string | - | 2048 | Yes |
| behaviorName_en_US | string | - | 2048 | Yes |
| behaviorName_es_ES | string | - | 2048 | Yes |
| behaviorName_fr_FR | string | - | 2048 | Yes |
| behaviorName_ja_JP | string | - | 2048 | Yes |
| behaviorName_ko_KR | string | - | 2048 | Yes |
| behaviorName_localized | string | - | 2048 | Yes |
| behaviorName_nl_NL | string | - | 2048 | Yes |
| behaviorName_pt_BR | string | - | 2048 | Yes |
| behaviorName_pt_PT | string | - | 2048 | Yes |
| behaviorName_ru_RU | string | - | 2048 | Yes |
| behaviorName_zh_CN | string | - | 2048 | Yes |
| behaviorName_zh_TW | string | - | 2048 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
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
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### CertificationContent

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| JobProfile_externalCode | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| entity | string | - | 128 | Yes |
| entityNav | Ref(CertificationEntity) | - | - | No |
| externalCode | string | - | 128 | No |
| jobProfileId | string | - | 255 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| order | string | int64 | - | Yes |
| sectionId | string | - | 255 | Yes |
| sectionType | string | - | 255 | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### CertificationEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| certification_de_DE | string | - | 256 | Yes |
| certification_defaultValue | string | - | 256 | Yes |
| certification_en_GB | string | - | 256 | Yes |
| certification_en_US | string | - | 256 | Yes |
| certification_es_ES | string | - | 256 | Yes |
| certification_fr_FR | string | - | 256 | Yes |
| certification_ja_JP | string | - | 256 | Yes |
| certification_ko_KR | string | - | 256 | Yes |
| certification_localized | string | - | 256 | Yes |
| certification_nl_NL | string | - | 256 | Yes |
| certification_pt_BR | string | - | 256 | Yes |
| certification_pt_PT | string | - | 256 | Yes |
| certification_ru_RU | string | - | 256 | Yes |
| certification_zh_CN | string | - | 256 | Yes |
| certification_zh_TW | string | - | 256 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
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
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### CompetencyContent

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| JobProfile_externalCode | string | - | 128 | No |
| competencyMappingId | string | - | 255 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| entity | string | - | 128 | Yes |
| entityNav | Ref(CompetencyEntity) | - | - | No |
| externalCode | string | - | 128 | No |
| jobProfileId | string | - | 255 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| order | string | int64 | - | Yes |
| role | string | - | 128 | Yes |
| roleNav | Ref(RoleEntity) | - | - | No |
| sectionId | string | - | 255 | Yes |
| sectionType | string | - | 255 | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### CompetencyEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| behaviors | Object | - | - | Yes |
| category_de_DE | string | - | 128 | Yes |
| category_defaultValue | string | - | 128 | Yes |
| category_en_GB | string | - | 128 | Yes |
| category_en_US | string | - | 128 | Yes |
| category_es_ES | string | - | 128 | Yes |
| category_fr_FR | string | - | 128 | Yes |
| category_ja_JP | string | - | 128 | Yes |
| category_ko_KR | string | - | 128 | Yes |
| category_localized | string | - | 128 | Yes |
| category_nl_NL | string | - | 128 | Yes |
| category_pt_BR | string | - | 128 | Yes |
| category_pt_PT | string | - | 128 | Yes |
| category_ru_RU | string | - | 128 | Yes |
| category_zh_CN | string | - | 128 | Yes |
| category_zh_TW | string | - | 128 | Yes |
| collection | boolean | - | - | Yes |
| competencies | Object | - | - | Yes |
| competencyTypes | Object | - | - | Yes |
| core | boolean | - | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
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
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| libName_de_DE | string | - | 64 | Yes |
| libName_defaultValue | string | - | 64 | Yes |
| libName_en_GB | string | - | 64 | Yes |
| libName_en_US | string | - | 64 | Yes |
| libName_es_ES | string | - | 64 | Yes |
| libName_fr_FR | string | - | 64 | Yes |
| libName_ja_JP | string | - | 64 | Yes |
| libName_ko_KR | string | - | 64 | Yes |
| libName_localized | string | - | 64 | Yes |
| libName_nl_NL | string | - | 64 | Yes |
| libName_pt_BR | string | - | 64 | Yes |
| libName_pt_PT | string | - | 64 | Yes |
| libName_ru_RU | string | - | 64 | Yes |
| libName_zh_CN | string | - | 64 | Yes |
| libName_zh_TW | string | - | 64 | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| name_de_DE | string | - | 2048 | Yes |
| name_defaultValue | string | - | 2048 | Yes |
| name_en_GB | string | - | 2048 | Yes |
| name_en_US | string | - | 2048 | Yes |
| name_es_ES | string | - | 2048 | Yes |
| name_fr_FR | string | - | 2048 | Yes |
| name_ja_JP | string | - | 2048 | Yes |
| name_ko_KR | string | - | 2048 | Yes |
| name_localized | string | - | 2048 | Yes |
| name_nl_NL | string | - | 2048 | Yes |
| name_pt_BR | string | - | 2048 | Yes |
| name_pt_PT | string | - | 2048 | Yes |
| name_ru_RU | string | - | 2048 | Yes |
| name_zh_CN | string | - | 2048 | Yes |
| name_zh_TW | string | - | 2048 | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### CompetencyType

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| GUID | string | int64 | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| name_de_DE | string | - | 255 | Yes |
| name_defaultValue | string | - | 255 | Yes |
| name_en_GB | string | - | 255 | Yes |
| name_en_US | string | - | 255 | Yes |
| name_es_ES | string | - | 255 | Yes |
| name_fr_FR | string | - | 255 | Yes |
| name_ja_JP | string | - | 255 | Yes |
| name_ko_KR | string | - | 255 | Yes |
| name_localized | string | - | 255 | Yes |
| name_nl_NL | string | - | 255 | Yes |
| name_pt_BR | string | - | 255 | Yes |
| name_pt_PT | string | - | 255 | Yes |
| name_ru_RU | string | - | 255 | Yes |
| name_zh_CN | string | - | 255 | Yes |
| name_zh_TW | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |

### EmploymentConditionContent

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| JobProfile_externalCode | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| entity | string | - | 128 | Yes |
| entityNav | Ref(EmploymentConditionEntity) | - | - | No |
| externalCode | string | - | 128 | No |
| jobProfileId | string | - | 255 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| order | string | int64 | - | Yes |
| sectionId | string | - | 255 | Yes |
| sectionType | string | - | 255 | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### EmploymentConditionEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| condition_de_DE | string | - | 256 | Yes |
| condition_defaultValue | string | - | 256 | Yes |
| condition_en_GB | string | - | 256 | Yes |
| condition_en_US | string | - | 256 | Yes |
| condition_es_ES | string | - | 256 | Yes |
| condition_fr_FR | string | - | 256 | Yes |
| condition_ja_JP | string | - | 256 | Yes |
| condition_ko_KR | string | - | 256 | Yes |
| condition_localized | string | - | 256 | Yes |
| condition_nl_NL | string | - | 256 | Yes |
| condition_pt_BR | string | - | 256 | Yes |
| condition_pt_PT | string | - | 256 | Yes |
| condition_ru_RU | string | - | 256 | Yes |
| condition_zh_CN | string | - | 256 | Yes |
| condition_zh_TW | string | - | 256 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### FamilyCompetencyMappingEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| FamilyEntity_externalCode | string | - | 128 | No |
| competency | string | - | 128 | Yes |
| competencyNav | Ref(CompetencyEntity) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### FamilyEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| competencies | Object | - | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| createdLocale | string | - | 255 | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| name_de_DE | string | - | 128 | Yes |
| name_defaultValue | string | - | 128 | Yes |
| name_en_GB | string | - | 128 | Yes |
| name_en_US | string | - | 128 | Yes |
| name_es_ES | string | - | 128 | Yes |
| name_fr_FR | string | - | 128 | Yes |
| name_ja_JP | string | - | 128 | Yes |
| name_ko_KR | string | - | 128 | Yes |
| name_localized | string | - | 128 | Yes |
| name_nl_NL | string | - | 128 | Yes |
| name_pt_BR | string | - | 128 | Yes |
| name_pt_PT | string | - | 128 | Yes |
| name_ru_RU | string | - | 128 | Yes |
| name_zh_CN | string | - | 128 | Yes |
| name_zh_TW | string | - | 128 | Yes |
| skills | Object | - | - | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### FamilySkillMappingEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| FamilyEntity_externalCode | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| proLevel_de_DE | string | - | 255 | Yes |
| proLevel_defaultValue | string | - | 255 | Yes |
| proLevel_en_GB | string | - | 255 | Yes |
| proLevel_en_US | string | - | 255 | Yes |
| proLevel_es_ES | string | - | 255 | Yes |
| proLevel_fr_FR | string | - | 255 | Yes |
| proLevel_ja_JP | string | - | 255 | Yes |
| proLevel_ko_KR | string | - | 255 | Yes |
| proLevel_localized | string | - | 255 | Yes |
| proLevel_nl_NL | string | - | 255 | Yes |
| proLevel_pt_BR | string | - | 255 | Yes |
| proLevel_pt_PT | string | - | 255 | Yes |
| proLevel_ru_RU | string | - | 255 | Yes |
| proLevel_zh_CN | string | - | 255 | Yes |
| proLevel_zh_TW | string | - | 255 | Yes |
| skill | string | - | 128 | Yes |
| skillNav | Ref(SkillEntity) | - | - | No |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### InterviewQuestionContent

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| JobProfile_externalCode | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| entity | string | - | 128 | Yes |
| entityNav | Ref(InterviewQuestionEntity) | - | - | No |
| externalCode | string | - | 128 | No |
| jobProfileId | string | - | 255 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| order | string | int64 | - | Yes |
| sectionId | string | - | 255 | Yes |
| sectionType | string | - | 255 | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### InterviewQuestionEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| question_de_DE | string | - | 256 | Yes |
| question_defaultValue | string | - | 256 | Yes |
| question_en_GB | string | - | 256 | Yes |
| question_en_US | string | - | 256 | Yes |
| question_es_ES | string | - | 256 | Yes |
| question_fr_FR | string | - | 256 | Yes |
| question_ja_JP | string | - | 256 | Yes |
| question_ko_KR | string | - | 256 | Yes |
| question_localized | string | - | 256 | Yes |
| question_nl_NL | string | - | 256 | Yes |
| question_pt_BR | string | - | 256 | Yes |
| question_pt_PT | string | - | 256 | Yes |
| question_ru_RU | string | - | 256 | Yes |
| question_zh_CN | string | - | 256 | Yes |
| question_zh_TW | string | - | 256 | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |
| type_de_DE | string | - | 128 | Yes |
| type_defaultValue | string | - | 128 | Yes |
| type_en_GB | string | - | 128 | Yes |
| type_en_US | string | - | 128 | Yes |
| type_es_ES | string | - | 128 | Yes |
| type_fr_FR | string | - | 128 | Yes |
| type_ja_JP | string | - | 128 | Yes |
| type_ko_KR | string | - | 128 | Yes |
| type_localized | string | - | 128 | Yes |
| type_nl_NL | string | - | 128 | Yes |
| type_pt_BR | string | - | 128 | Yes |
| type_pt_PT | string | - | 128 | Yes |
| type_ru_RU | string | - | 128 | Yes |
| type_zh_CN | string | - | 128 | Yes |
| type_zh_TW | string | - | 128 | Yes |

### JDTemplateFamilyMapping

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| JobDescTemplate_externalCode | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| family | string | - | 128 | Yes |
| familyNav | Ref(FamilyEntity) | - | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### JobCodeMappingEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| RoleEntity_externalCode | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| jobCode | string | - | 255 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |
| type | string | - | 255 | Yes |
| usage | string | int64 | - | Yes |

### JobDescSection

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| JobDescTemplate_externalCode | string | - | 128 | No |
| bold | boolean | - | - | Yes |
| boldHeader | boolean | - | - | Yes |
| bulletType | string | - | 255 | Yes |
| contentType | string | - | 255 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| externalPosting | boolean | - | - | Yes |
| fontSizeHeader | string | int64 | - | Yes |
| internalPosting | boolean | - | - | Yes |
| italic | boolean | - | - | Yes |
| italicHeader | boolean | - | - | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| multiContents | boolean | - | - | Yes |
| name_de_DE | string | - | 100 | Yes |
| name_defaultValue | string | - | 100 | Yes |
| name_en_GB | string | - | 100 | Yes |
| name_en_US | string | - | 100 | Yes |
| name_es_ES | string | - | 100 | Yes |
| name_fr_FR | string | - | 100 | Yes |
| name_ja_JP | string | - | 100 | Yes |
| name_ko_KR | string | - | 100 | Yes |
| name_localized | string | - | 100 | Yes |
| name_nl_NL | string | - | 100 | Yes |
| name_pt_BR | string | - | 100 | Yes |
| name_pt_PT | string | - | 100 | Yes |
| name_ru_RU | string | - | 100 | Yes |
| name_zh_CN | string | - | 100 | Yes |
| name_zh_TW | string | - | 100 | Yes |
| onlyAdmin | boolean | - | - | Yes |
| order | string | int64 | - | Yes |
| removable | boolean | - | - | Yes |
| required | boolean | - | - | Yes |
| showInJobReq | boolean | - | - | Yes |
| smallIcon | string | - | 255 | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |
| underline | boolean | - | - | Yes |
| underlineHeader | boolean | - | - | Yes |

### JobDescTemplate

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| footer_de_DE | string | - | 50 | Yes |
| footer_defaultValue | string | - | 50 | Yes |
| footer_en_GB | string | - | 50 | Yes |
| footer_en_US | string | - | 50 | Yes |
| footer_es_ES | string | - | 50 | Yes |
| footer_fr_FR | string | - | 50 | Yes |
| footer_ja_JP | string | - | 50 | Yes |
| footer_ko_KR | string | - | 50 | Yes |
| footer_localized | string | - | 50 | Yes |
| footer_nl_NL | string | - | 50 | Yes |
| footer_pt_BR | string | - | 50 | Yes |
| footer_pt_PT | string | - | 50 | Yes |
| footer_ru_RU | string | - | 50 | Yes |
| footer_zh_CN | string | - | 50 | Yes |
| footer_zh_TW | string | - | 50 | Yes |
| header_de_DE | string | - | 50 | Yes |
| header_defaultValue | string | - | 50 | Yes |
| header_en_GB | string | - | 50 | Yes |
| header_en_US | string | - | 50 | Yes |
| header_es_ES | string | - | 50 | Yes |
| header_fr_FR | string | - | 50 | Yes |
| header_ja_JP | string | - | 50 | Yes |
| header_ko_KR | string | - | 50 | Yes |
| header_localized | string | - | 50 | Yes |
| header_nl_NL | string | - | 50 | Yes |
| header_pt_BR | string | - | 50 | Yes |
| header_pt_PT | string | - | 50 | Yes |
| header_ru_RU | string | - | 50 | Yes |
| header_zh_CN | string | - | 50 | Yes |
| header_zh_TW | string | - | 50 | Yes |
| jdFamilyMappings | Object | - | - | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| sections | Object | - | - | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| title_de_DE | string | - | 100 | Yes |
| title_defaultValue | string | - | 100 | Yes |
| title_en_GB | string | - | 100 | Yes |
| title_en_US | string | - | 100 | Yes |
| title_es_ES | string | - | 100 | Yes |
| title_fr_FR | string | - | 100 | Yes |
| title_ja_JP | string | - | 100 | Yes |
| title_ko_KR | string | - | 100 | Yes |
| title_localized | string | - | 100 | Yes |
| title_nl_NL | string | - | 100 | Yes |
| title_pt_BR | string | - | 100 | Yes |
| title_pt_PT | string | - | 100 | Yes |
| title_ru_RU | string | - | 100 | Yes |
| title_zh_CN | string | - | 100 | Yes |
| title_zh_TW | string | - | 100 | Yes |
| transactionSequence | string | int64 | - | Yes |

### JobProfile

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| certificationContents | Object | - | - | Yes |
| compData_de_DE | string | - | 4000 | Yes |
| compData_defaultValue | string | - | 4000 | Yes |
| compData_en_GB | string | - | 4000 | Yes |
| compData_en_US | string | - | 4000 | Yes |
| compData_es_ES | string | - | 4000 | Yes |
| compData_fr_FR | string | - | 4000 | Yes |
| compData_ja_JP | string | - | 4000 | Yes |
| compData_ko_KR | string | - | 4000 | Yes |
| compData_localized | string | - | 4000 | Yes |
| compData_nl_NL | string | - | 4000 | Yes |
| compData_pt_BR | string | - | 4000 | Yes |
| compData_pt_PT | string | - | 4000 | Yes |
| compData_ru_RU | string | - | 4000 | Yes |
| compData_zh_CN | string | - | 4000 | Yes |
| compData_zh_TW | string | - | 4000 | Yes |
| compDatas | Object | - | - | Yes |
| competencyContents | Object | - | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| draft | boolean | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| employmentConditionContents | Object | - | - | Yes |
| externalCode | string | - | 128 | No |
| footer_de_DE | string | - | 4000 | Yes |
| footer_defaultValue | string | - | 4000 | Yes |
| footer_en_GB | string | - | 4000 | Yes |
| footer_en_US | string | - | 4000 | Yes |
| footer_es_ES | string | - | 4000 | Yes |
| footer_fr_FR | string | - | 4000 | Yes |
| footer_ja_JP | string | - | 4000 | Yes |
| footer_ko_KR | string | - | 4000 | Yes |
| footer_localized | string | - | 4000 | Yes |
| footer_nl_NL | string | - | 4000 | Yes |
| footer_pt_BR | string | - | 4000 | Yes |
| footer_pt_PT | string | - | 4000 | Yes |
| footer_ru_RU | string | - | 4000 | Yes |
| footer_zh_CN | string | - | 4000 | Yes |
| footer_zh_TW | string | - | 4000 | Yes |
| footers | Object | - | - | Yes |
| header_de_DE | string | - | 4000 | Yes |
| header_defaultValue | string | - | 4000 | Yes |
| header_en_GB | string | - | 4000 | Yes |
| header_en_US | string | - | 4000 | Yes |
| header_es_ES | string | - | 4000 | Yes |
| header_fr_FR | string | - | 4000 | Yes |
| header_ja_JP | string | - | 4000 | Yes |
| header_ko_KR | string | - | 4000 | Yes |
| header_localized | string | - | 4000 | Yes |
| header_nl_NL | string | - | 4000 | Yes |
| header_pt_BR | string | - | 4000 | Yes |
| header_pt_PT | string | - | 4000 | Yes |
| header_ru_RU | string | - | 4000 | Yes |
| header_zh_CN | string | - | 4000 | Yes |
| header_zh_TW | string | - | 4000 | Yes |
| headers | Object | - | - | Yes |
| interviewQuestionContents | Object | - | - | Yes |
| jobResponsibilityContents | Object | - | - | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| lastModifierName | string | - | 255 | Yes |
| longDesc_de_DE | string | - | 4000 | Yes |
| longDesc_defaultValue | string | - | 4000 | Yes |
| longDesc_en_GB | string | - | 4000 | Yes |
| longDesc_en_US | string | - | 4000 | Yes |
| longDesc_es_ES | string | - | 4000 | Yes |
| longDesc_fr_FR | string | - | 4000 | Yes |
| longDesc_ja_JP | string | - | 4000 | Yes |
| longDesc_ko_KR | string | - | 4000 | Yes |
| longDesc_localized | string | - | 4000 | Yes |
| longDesc_nl_NL | string | - | 4000 | Yes |
| longDesc_pt_BR | string | - | 4000 | Yes |
| longDesc_pt_PT | string | - | 4000 | Yes |
| longDesc_ru_RU | string | - | 4000 | Yes |
| longDesc_zh_CN | string | - | 4000 | Yes |
| longDesc_zh_TW | string | - | 4000 | Yes |
| longDesciptions | Object | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| migrated | boolean | - | - | Yes |
| name_de_DE | string | - | 100 | Yes |
| name_defaultValue | string | - | 100 | Yes |
| name_en_GB | string | - | 100 | Yes |
| name_en_US | string | - | 100 | Yes |
| name_es_ES | string | - | 100 | Yes |
| name_fr_FR | string | - | 100 | Yes |
| name_ja_JP | string | - | 100 | Yes |
| name_ko_KR | string | - | 100 | Yes |
| name_localized | string | - | 100 | Yes |
| name_nl_NL | string | - | 100 | Yes |
| name_pt_BR | string | - | 100 | Yes |
| name_pt_PT | string | - | 100 | Yes |
| name_ru_RU | string | - | 100 | Yes |
| name_zh_CN | string | - | 100 | Yes |
| name_zh_TW | string | - | 100 | Yes |
| physicalReqContents | Object | - | - | Yes |
| position | string | - | 128 | Yes |
| relevantIndustryContents | Object | - | - | Yes |
| role | string | - | 128 | Yes |
| roleNav | Ref(RoleEntity) | - | - | No |
| shortDesc_de_DE | string | - | 4000 | Yes |
| shortDesc_defaultValue | string | - | 4000 | Yes |
| shortDesc_en_GB | string | - | 4000 | Yes |
| shortDesc_en_US | string | - | 4000 | Yes |
| shortDesc_es_ES | string | - | 4000 | Yes |
| shortDesc_fr_FR | string | - | 4000 | Yes |
| shortDesc_ja_JP | string | - | 4000 | Yes |
| shortDesc_ko_KR | string | - | 4000 | Yes |
| shortDesc_localized | string | - | 4000 | Yes |
| shortDesc_nl_NL | string | - | 4000 | Yes |
| shortDesc_pt_BR | string | - | 4000 | Yes |
| shortDesc_pt_PT | string | - | 4000 | Yes |
| shortDesc_ru_RU | string | - | 4000 | Yes |
| shortDesc_zh_CN | string | - | 4000 | Yes |
| shortDesc_zh_TW | string | - | 4000 | Yes |
| shortDesciptions | Object | - | - | Yes |
| skillContents | Object | - | - | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| template | string | - | 128 | Yes |
| templateNav | Ref(JobDescTemplate) | - | - | No |
| transactionSequence | string | int64 | - | Yes |

### JobProfileLocalizedData

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| JobProfile_externalCode | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| desc_de_DE | string | - | 4000 | Yes |
| desc_defaultValue | string | - | 4000 | Yes |
| desc_en_GB | string | - | 4000 | Yes |
| desc_en_US | string | - | 4000 | Yes |
| desc_es_ES | string | - | 4000 | Yes |
| desc_fr_FR | string | - | 4000 | Yes |
| desc_ja_JP | string | - | 4000 | Yes |
| desc_ko_KR | string | - | 4000 | Yes |
| desc_localized | string | - | 4000 | Yes |
| desc_nl_NL | string | - | 4000 | Yes |
| desc_pt_BR | string | - | 4000 | Yes |
| desc_pt_PT | string | - | 4000 | Yes |
| desc_ru_RU | string | - | 4000 | Yes |
| desc_zh_CN | string | - | 4000 | Yes |
| desc_zh_TW | string | - | 4000 | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| sectionId | string | - | 255 | Yes |
| sectionType | string | - | 255 | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### JobResponsibilityContent

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| JobProfile_externalCode | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| entity | string | - | 128 | Yes |
| entityNav | Ref(JobResponsibilityEntity) | - | - | No |
| externalCode | string | - | 128 | No |
| jobProfileId | string | - | 255 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| order | string | int64 | - | Yes |
| sectionId | string | - | 255 | Yes |
| sectionType | string | - | 255 | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### JobResponsibilityEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| category_de_DE | string | - | 128 | Yes |
| category_defaultValue | string | - | 128 | Yes |
| category_en_GB | string | - | 128 | Yes |
| category_en_US | string | - | 128 | Yes |
| category_es_ES | string | - | 128 | Yes |
| category_fr_FR | string | - | 128 | Yes |
| category_ja_JP | string | - | 128 | Yes |
| category_ko_KR | string | - | 128 | Yes |
| category_localized | string | - | 128 | Yes |
| category_nl_NL | string | - | 128 | Yes |
| category_pt_BR | string | - | 128 | Yes |
| category_pt_PT | string | - | 128 | Yes |
| category_ru_RU | string | - | 128 | Yes |
| category_zh_CN | string | - | 128 | Yes |
| category_zh_TW | string | - | 128 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
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
| duty_de_DE | string | - | 4000 | Yes |
| duty_defaultValue | string | - | 4000 | Yes |
| duty_en_GB | string | - | 4000 | Yes |
| duty_en_US | string | - | 4000 | Yes |
| duty_es_ES | string | - | 4000 | Yes |
| duty_fr_FR | string | - | 4000 | Yes |
| duty_ja_JP | string | - | 4000 | Yes |
| duty_ko_KR | string | - | 4000 | Yes |
| duty_localized | string | - | 4000 | Yes |
| duty_nl_NL | string | - | 4000 | Yes |
| duty_pt_BR | string | - | 4000 | Yes |
| duty_pt_PT | string | - | 4000 | Yes |
| duty_ru_RU | string | - | 4000 | Yes |
| duty_zh_CN | string | - | 4000 | Yes |
| duty_zh_TW | string | - | 4000 | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| libName_de_DE | string | - | 128 | Yes |
| libName_defaultValue | string | - | 128 | Yes |
| libName_en_GB | string | - | 128 | Yes |
| libName_en_US | string | - | 128 | Yes |
| libName_es_ES | string | - | 128 | Yes |
| libName_fr_FR | string | - | 128 | Yes |
| libName_ja_JP | string | - | 128 | Yes |
| libName_ko_KR | string | - | 128 | Yes |
| libName_localized | string | - | 128 | Yes |
| libName_nl_NL | string | - | 128 | Yes |
| libName_pt_BR | string | - | 128 | Yes |
| libName_pt_PT | string | - | 128 | Yes |
| libName_ru_RU | string | - | 128 | Yes |
| libName_zh_CN | string | - | 128 | Yes |
| libName_zh_TW | string | - | 128 | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| name_de_DE | string | - | 256 | Yes |
| name_defaultValue | string | - | 256 | Yes |
| name_en_GB | string | - | 256 | Yes |
| name_en_US | string | - | 256 | Yes |
| name_es_ES | string | - | 256 | Yes |
| name_fr_FR | string | - | 256 | Yes |
| name_ja_JP | string | - | 256 | Yes |
| name_ko_KR | string | - | 256 | Yes |
| name_localized | string | - | 256 | Yes |
| name_nl_NL | string | - | 256 | Yes |
| name_pt_BR | string | - | 256 | Yes |
| name_pt_PT | string | - | 256 | Yes |
| name_ru_RU | string | - | 256 | Yes |
| name_zh_CN | string | - | 256 | Yes |
| name_zh_TW | string | - | 256 | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### PhysicalReqContent

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| JobProfile_externalCode | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| entity | string | - | 128 | Yes |
| entityNav | Ref(PhysicalReqEntity) | - | - | No |
| externalCode | string | - | 128 | No |
| jobProfileId | string | - | 255 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| order | string | int64 | - | Yes |
| sectionId | string | - | 255 | Yes |
| sectionType | string | - | 255 | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### PhysicalReqEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
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
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| requirement_de_DE | string | - | 256 | Yes |
| requirement_defaultValue | string | - | 256 | Yes |
| requirement_en_GB | string | - | 256 | Yes |
| requirement_en_US | string | - | 256 | Yes |
| requirement_es_ES | string | - | 256 | Yes |
| requirement_fr_FR | string | - | 256 | Yes |
| requirement_ja_JP | string | - | 256 | Yes |
| requirement_ko_KR | string | - | 256 | Yes |
| requirement_localized | string | - | 256 | Yes |
| requirement_nl_NL | string | - | 256 | Yes |
| requirement_pt_BR | string | - | 256 | Yes |
| requirement_pt_PT | string | - | 256 | Yes |
| requirement_ru_RU | string | - | 256 | Yes |
| requirement_zh_CN | string | - | 256 | Yes |
| requirement_zh_TW | string | - | 256 | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### PositionCompetencyMappingEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PositionEntity_externalCode | string | - | 128 | No |
| competency | string | - | 128 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| rating_de_DE | string | - | 255 | Yes |
| rating_defaultValue | string | - | 255 | Yes |
| rating_en_GB | string | - | 255 | Yes |
| rating_en_US | string | - | 255 | Yes |
| rating_es_ES | string | - | 255 | Yes |
| rating_fr_FR | string | - | 255 | Yes |
| rating_ja_JP | string | - | 255 | Yes |
| rating_ko_KR | string | - | 255 | Yes |
| rating_localized | string | - | 255 | Yes |
| rating_nl_NL | string | - | 255 | Yes |
| rating_pt_BR | string | - | 255 | Yes |
| rating_pt_PT | string | - | 255 | Yes |
| rating_ru_RU | string | - | 255 | Yes |
| rating_zh_CN | string | - | 255 | Yes |
| rating_zh_TW | string | - | 255 | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |
| weight_de_DE | string | - | 255 | Yes |
| weight_defaultValue | string | - | 255 | Yes |
| weight_en_GB | string | - | 255 | Yes |
| weight_en_US | string | - | 255 | Yes |
| weight_es_ES | string | - | 255 | Yes |
| weight_fr_FR | string | - | 255 | Yes |
| weight_ja_JP | string | - | 255 | Yes |
| weight_ko_KR | string | - | 255 | Yes |
| weight_localized | string | - | 255 | Yes |
| weight_nl_NL | string | - | 255 | Yes |
| weight_pt_BR | string | - | 255 | Yes |
| weight_pt_PT | string | - | 255 | Yes |
| weight_ru_RU | string | - | 255 | Yes |
| weight_zh_CN | string | - | 255 | Yes |
| weight_zh_TW | string | - | 255 | Yes |

### PositionEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| jobCode | string | - | 255 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| position | string | - | 128 | Yes |
| positionCompetencyMappings | Object | - | - | Yes |
| positionSkillMappings | Object | - | - | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### PositionSkillMappingEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PositionEntity_externalCode | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| proLevel_de_DE | string | - | 255 | Yes |
| proLevel_defaultValue | string | - | 255 | Yes |
| proLevel_en_GB | string | - | 255 | Yes |
| proLevel_en_US | string | - | 255 | Yes |
| proLevel_es_ES | string | - | 255 | Yes |
| proLevel_fr_FR | string | - | 255 | Yes |
| proLevel_ja_JP | string | - | 255 | Yes |
| proLevel_ko_KR | string | - | 255 | Yes |
| proLevel_localized | string | - | 255 | Yes |
| proLevel_nl_NL | string | - | 255 | Yes |
| proLevel_pt_BR | string | - | 255 | Yes |
| proLevel_pt_PT | string | - | 255 | Yes |
| proLevel_ru_RU | string | - | 255 | Yes |
| proLevel_zh_CN | string | - | 255 | Yes |
| proLevel_zh_TW | string | - | 255 | Yes |
| skill | string | - | 128 | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### RatedSkillMapping

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| SkillProfile_externalCode | string | - | 100 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| expectedLevel_de_DE | string | - | 255 | Yes |
| expectedLevel_defaultValue | string | - | 255 | Yes |
| expectedLevel_en_GB | string | - | 255 | Yes |
| expectedLevel_en_US | string | - | 255 | Yes |
| expectedLevel_es_ES | string | - | 255 | Yes |
| expectedLevel_fr_FR | string | - | 255 | Yes |
| expectedLevel_ja_JP | string | - | 255 | Yes |
| expectedLevel_ko_KR | string | - | 255 | Yes |
| expectedLevel_localized | string | - | 255 | Yes |
| expectedLevel_nl_NL | string | - | 255 | Yes |
| expectedLevel_pt_BR | string | - | 255 | Yes |
| expectedLevel_pt_PT | string | - | 255 | Yes |
| expectedLevel_ru_RU | string | - | 255 | Yes |
| expectedLevel_zh_CN | string | - | 255 | Yes |
| expectedLevel_zh_TW | string | - | 255 | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| managerRatedLevel_de_DE | string | - | 255 | Yes |
| managerRatedLevel_defaultValue | string | - | 255 | Yes |
| managerRatedLevel_en_GB | string | - | 255 | Yes |
| managerRatedLevel_en_US | string | - | 255 | Yes |
| managerRatedLevel_es_ES | string | - | 255 | Yes |
| managerRatedLevel_fr_FR | string | - | 255 | Yes |
| managerRatedLevel_ja_JP | string | - | 255 | Yes |
| managerRatedLevel_ko_KR | string | - | 255 | Yes |
| managerRatedLevel_localized | string | - | 255 | Yes |
| managerRatedLevel_nl_NL | string | - | 255 | Yes |
| managerRatedLevel_pt_BR | string | - | 255 | Yes |
| managerRatedLevel_pt_PT | string | - | 255 | Yes |
| managerRatedLevel_ru_RU | string | - | 255 | Yes |
| managerRatedLevel_zh_CN | string | - | 255 | Yes |
| managerRatedLevel_zh_TW | string | - | 255 | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| selfRatedLevel_de_DE | string | - | 255 | Yes |
| selfRatedLevel_defaultValue | string | - | 255 | Yes |
| selfRatedLevel_en_GB | string | - | 255 | Yes |
| selfRatedLevel_en_US | string | - | 255 | Yes |
| selfRatedLevel_es_ES | string | - | 255 | Yes |
| selfRatedLevel_fr_FR | string | - | 255 | Yes |
| selfRatedLevel_ja_JP | string | - | 255 | Yes |
| selfRatedLevel_ko_KR | string | - | 255 | Yes |
| selfRatedLevel_localized | string | - | 255 | Yes |
| selfRatedLevel_nl_NL | string | - | 255 | Yes |
| selfRatedLevel_pt_BR | string | - | 255 | Yes |
| selfRatedLevel_pt_PT | string | - | 255 | Yes |
| selfRatedLevel_ru_RU | string | - | 255 | Yes |
| selfRatedLevel_zh_CN | string | - | 255 | Yes |
| selfRatedLevel_zh_TW | string | - | 255 | Yes |
| skill | string | - | 128 | Yes |
| skillNav | Ref(SkillEntity) | - | - | No |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### RelevantIndustryContent

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| JobProfile_externalCode | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| entity | string | - | 128 | Yes |
| entityNav | Ref(RelevantIndustryEntity) | - | - | No |
| externalCode | string | - | 128 | No |
| jobProfileId | string | - | 255 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| order | string | int64 | - | Yes |
| sectionId | string | - | 255 | Yes |
| sectionType | string | - | 255 | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### RelevantIndustryEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| industry_de_DE | string | - | 256 | Yes |
| industry_defaultValue | string | - | 256 | Yes |
| industry_en_GB | string | - | 256 | Yes |
| industry_en_US | string | - | 256 | Yes |
| industry_es_ES | string | - | 256 | Yes |
| industry_fr_FR | string | - | 256 | Yes |
| industry_ja_JP | string | - | 256 | Yes |
| industry_ko_KR | string | - | 256 | Yes |
| industry_localized | string | - | 256 | Yes |
| industry_nl_NL | string | - | 256 | Yes |
| industry_pt_BR | string | - | 256 | Yes |
| industry_pt_PT | string | - | 256 | Yes |
| industry_ru_RU | string | - | 256 | Yes |
| industry_zh_CN | string | - | 256 | Yes |
| industry_zh_TW | string | - | 256 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### RoleCompetencyBehaviorMappingEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| RoleEntity_externalCode | string | - | 128 | No |
| behaviorMappingEntity | string | - | 128 | Yes |
| behaviorMappingEntityNav | Ref(BehaviorMappingEntity) | - | - | No |
| competency | string | - | 128 | Yes |
| competencyNav | Ref(CompetencyEntity) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| rating_de_DE | string | - | 255 | Yes |
| rating_defaultValue | string | - | 255 | Yes |
| rating_en_DEBUG | string | - | 255 | Yes |
| rating_en_GB | string | - | 255 | Yes |
| rating_en_US | string | - | 255 | Yes |
| rating_es_ES | string | - | 255 | Yes |
| rating_fr_FR | string | - | 255 | Yes |
| rating_ja_JP | string | - | 255 | Yes |
| rating_ko_KR | string | - | 255 | Yes |
| rating_localized | string | - | 255 | Yes |
| rating_nl_NL | string | - | 255 | Yes |
| rating_pt_BR | string | - | 255 | Yes |
| rating_pt_PT | string | - | 255 | Yes |
| rating_ru_RU | string | - | 255 | Yes |
| rating_zh_CN | string | - | 255 | Yes |
| rating_zh_TW | string | - | 255 | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| weight_de_DE | string | - | 255 | Yes |
| weight_defaultValue | string | - | 255 | Yes |
| weight_en_DEBUG | string | - | 255 | Yes |
| weight_en_GB | string | - | 255 | Yes |
| weight_en_US | string | - | 255 | Yes |
| weight_es_ES | string | - | 255 | Yes |
| weight_fr_FR | string | - | 255 | Yes |
| weight_ja_JP | string | - | 255 | Yes |
| weight_ko_KR | string | - | 255 | Yes |
| weight_localized | string | - | 255 | Yes |
| weight_nl_NL | string | - | 255 | Yes |
| weight_pt_BR | string | - | 255 | Yes |
| weight_pt_PT | string | - | 255 | Yes |
| weight_ru_RU | string | - | 255 | Yes |
| weight_zh_CN | string | - | 255 | Yes |
| weight_zh_TW | string | - | 255 | Yes |

### RoleCompetencyMappingEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| RoleEntity_externalCode | string | - | 128 | No |
| competency | string | - | 128 | Yes |
| competencyNav | Ref(CompetencyEntity) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| rating_de_DE | string | - | 255 | Yes |
| rating_defaultValue | string | - | 255 | Yes |
| rating_en_GB | string | - | 255 | Yes |
| rating_en_US | string | - | 255 | Yes |
| rating_es_ES | string | - | 255 | Yes |
| rating_fr_FR | string | - | 255 | Yes |
| rating_ja_JP | string | - | 255 | Yes |
| rating_ko_KR | string | - | 255 | Yes |
| rating_localized | string | - | 255 | Yes |
| rating_nl_NL | string | - | 255 | Yes |
| rating_pt_BR | string | - | 255 | Yes |
| rating_pt_PT | string | - | 255 | Yes |
| rating_ru_RU | string | - | 255 | Yes |
| rating_zh_CN | string | - | 255 | Yes |
| rating_zh_TW | string | - | 255 | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |
| weight_de_DE | string | - | 255 | Yes |
| weight_defaultValue | string | - | 255 | Yes |
| weight_en_GB | string | - | 255 | Yes |
| weight_en_US | string | - | 255 | Yes |
| weight_es_ES | string | - | 255 | Yes |
| weight_fr_FR | string | - | 255 | Yes |
| weight_ja_JP | string | - | 255 | Yes |
| weight_ko_KR | string | - | 255 | Yes |
| weight_localized | string | - | 255 | Yes |
| weight_nl_NL | string | - | 255 | Yes |
| weight_pt_BR | string | - | 255 | Yes |
| weight_pt_PT | string | - | 255 | Yes |
| weight_ru_RU | string | - | 255 | Yes |
| weight_zh_CN | string | - | 255 | Yes |
| weight_zh_TW | string | - | 255 | Yes |

### RoleEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| createdLocale | string | - | 255 | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| family | string | - | 128 | Yes |
| familyNav | Ref(FamilyEntity) | - | - | No |
| jobCodeMappings | Object | - | - | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| name_de_DE | string | - | 128 | Yes |
| name_defaultValue | string | - | 128 | Yes |
| name_en_GB | string | - | 128 | Yes |
| name_en_US | string | - | 128 | Yes |
| name_es_ES | string | - | 128 | Yes |
| name_fr_FR | string | - | 128 | Yes |
| name_ja_JP | string | - | 128 | Yes |
| name_ko_KR | string | - | 128 | Yes |
| name_localized | string | - | 128 | Yes |
| name_nl_NL | string | - | 128 | Yes |
| name_pt_BR | string | - | 128 | Yes |
| name_pt_PT | string | - | 128 | Yes |
| name_ru_RU | string | - | 128 | Yes |
| name_zh_CN | string | - | 128 | Yes |
| name_zh_TW | string | - | 128 | Yes |
| roleCompetencyBehaviorMappings | Object | - | - | Yes |
| roleCompetencyMappings | Object | - | - | Yes |
| roleSkillMappings | Object | - | - | Yes |
| roleTalentPoolMappings | Object | - | - | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### RoleSkillMappingEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| RoleEntity_externalCode | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| proLevel_de_DE | string | - | 255 | Yes |
| proLevel_defaultValue | string | - | 255 | Yes |
| proLevel_en_GB | string | - | 255 | Yes |
| proLevel_en_US | string | - | 255 | Yes |
| proLevel_es_ES | string | - | 255 | Yes |
| proLevel_fr_FR | string | - | 255 | Yes |
| proLevel_ja_JP | string | - | 255 | Yes |
| proLevel_ko_KR | string | - | 255 | Yes |
| proLevel_localized | string | - | 255 | Yes |
| proLevel_nl_NL | string | - | 255 | Yes |
| proLevel_pt_BR | string | - | 255 | Yes |
| proLevel_pt_PT | string | - | 255 | Yes |
| proLevel_ru_RU | string | - | 255 | Yes |
| proLevel_zh_CN | string | - | 255 | Yes |
| proLevel_zh_TW | string | - | 255 | Yes |
| skill | string | - | 128 | Yes |
| skillNav | Ref(SkillEntity) | - | - | No |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### RoleTalentPoolMappingEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| RoleEntity_externalCode | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| description | string | - | 4000 | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| mdfTalentPool | string | - | 128 | Yes |
| name | string | - | 128 | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| talentPoolId | string | int64 | - | Yes |
| transactionSequence | string | int64 | - | Yes |

### SelfReportSkillMapping

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| SkillProfile_externalCode | string | - | 100 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| skill | string | - | 128 | Yes |
| skillNav | Ref(SkillEntity) | - | - | No |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### SkillContent

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| JobProfile_externalCode | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| entity | string | - | 128 | Yes |
| entityNav | Ref(SkillEntity) | - | - | No |
| externalCode | string | - | 128 | No |
| jobProfileId | string | - | 255 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| order | string | int64 | - | Yes |
| proLevel_de_DE | string | - | 255 | Yes |
| proLevel_defaultValue | string | - | 255 | Yes |
| proLevel_en_GB | string | - | 255 | Yes |
| proLevel_en_US | string | - | 255 | Yes |
| proLevel_es_ES | string | - | 255 | Yes |
| proLevel_fr_FR | string | - | 255 | Yes |
| proLevel_ja_JP | string | - | 255 | Yes |
| proLevel_ko_KR | string | - | 255 | Yes |
| proLevel_localized | string | - | 255 | Yes |
| proLevel_nl_NL | string | - | 255 | Yes |
| proLevel_pt_BR | string | - | 255 | Yes |
| proLevel_pt_PT | string | - | 255 | Yes |
| proLevel_ru_RU | string | - | 255 | Yes |
| proLevel_zh_CN | string | - | 255 | Yes |
| proLevel_zh_TW | string | - | 255 | Yes |
| role | string | - | 128 | Yes |
| roleNav | Ref(RoleEntity) | - | - | No |
| sectionId | string | - | 255 | Yes |
| sectionType | string | - | 255 | Yes |
| skillMappingId | string | - | 255 | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### SkillEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| category_de_DE | string | - | 128 | Yes |
| category_defaultValue | string | - | 128 | Yes |
| category_en_GB | string | - | 128 | Yes |
| category_en_US | string | - | 128 | Yes |
| category_es_ES | string | - | 128 | Yes |
| category_fr_FR | string | - | 128 | Yes |
| category_ja_JP | string | - | 128 | Yes |
| category_ko_KR | string | - | 128 | Yes |
| category_localized | string | - | 128 | Yes |
| category_nl_NL | string | - | 128 | Yes |
| category_pt_BR | string | - | 128 | Yes |
| category_pt_PT | string | - | 128 | Yes |
| category_ru_RU | string | - | 128 | Yes |
| category_zh_CN | string | - | 128 | Yes |
| category_zh_TW | string | - | 128 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| definition_de_DE | string | - | 4000 | Yes |
| definition_defaultValue | string | - | 4000 | Yes |
| definition_en_GB | string | - | 4000 | Yes |
| definition_en_US | string | - | 4000 | Yes |
| definition_es_ES | string | - | 4000 | Yes |
| definition_fr_FR | string | - | 4000 | Yes |
| definition_ja_JP | string | - | 4000 | Yes |
| definition_ko_KR | string | - | 4000 | Yes |
| definition_localized | string | - | 4000 | Yes |
| definition_nl_NL | string | - | 4000 | Yes |
| definition_pt_BR | string | - | 4000 | Yes |
| definition_pt_PT | string | - | 4000 | Yes |
| definition_ru_RU | string | - | 4000 | Yes |
| definition_zh_CN | string | - | 4000 | Yes |
| definition_zh_TW | string | - | 4000 | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| group_de_DE | string | - | 128 | Yes |
| group_defaultValue | string | - | 128 | Yes |
| group_en_GB | string | - | 128 | Yes |
| group_en_US | string | - | 128 | Yes |
| group_es_ES | string | - | 128 | Yes |
| group_fr_FR | string | - | 128 | Yes |
| group_ja_JP | string | - | 128 | Yes |
| group_ko_KR | string | - | 128 | Yes |
| group_localized | string | - | 128 | Yes |
| group_nl_NL | string | - | 128 | Yes |
| group_pt_BR | string | - | 128 | Yes |
| group_pt_PT | string | - | 128 | Yes |
| group_ru_RU | string | - | 128 | Yes |
| group_zh_CN | string | - | 128 | Yes |
| group_zh_TW | string | - | 128 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| libName_de_DE | string | - | 128 | Yes |
| libName_defaultValue | string | - | 128 | Yes |
| libName_en_GB | string | - | 128 | Yes |
| libName_en_US | string | - | 128 | Yes |
| libName_es_ES | string | - | 128 | Yes |
| libName_fr_FR | string | - | 128 | Yes |
| libName_ja_JP | string | - | 128 | Yes |
| libName_ko_KR | string | - | 128 | Yes |
| libName_localized | string | - | 128 | Yes |
| libName_nl_NL | string | - | 128 | Yes |
| libName_pt_BR | string | - | 128 | Yes |
| libName_pt_PT | string | - | 128 | Yes |
| libName_ru_RU | string | - | 128 | Yes |
| libName_zh_CN | string | - | 128 | Yes |
| libName_zh_TW | string | - | 128 | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| name_de_DE | string | - | 256 | Yes |
| name_defaultValue | string | - | 256 | Yes |
| name_en_GB | string | - | 256 | Yes |
| name_en_US | string | - | 256 | Yes |
| name_es_ES | string | - | 256 | Yes |
| name_fr_FR | string | - | 256 | Yes |
| name_ja_JP | string | - | 256 | Yes |
| name_ko_KR | string | - | 256 | Yes |
| name_localized | string | - | 256 | Yes |
| name_nl_NL | string | - | 256 | Yes |
| name_pt_BR | string | - | 256 | Yes |
| name_pt_PT | string | - | 256 | Yes |
| name_ru_RU | string | - | 256 | Yes |
| name_zh_CN | string | - | 256 | Yes |
| name_zh_TW | string | - | 256 | Yes |
| proLevel1_de_DE | string | - | 4000 | Yes |
| proLevel1_defaultValue | string | - | 4000 | Yes |
| proLevel1_en_GB | string | - | 4000 | Yes |
| proLevel1_en_US | string | - | 4000 | Yes |
| proLevel1_es_ES | string | - | 4000 | Yes |
| proLevel1_fr_FR | string | - | 4000 | Yes |
| proLevel1_ja_JP | string | - | 4000 | Yes |
| proLevel1_ko_KR | string | - | 4000 | Yes |
| proLevel1_localized | string | - | 4000 | Yes |
| proLevel1_nl_NL | string | - | 4000 | Yes |
| proLevel1_pt_BR | string | - | 4000 | Yes |
| proLevel1_pt_PT | string | - | 4000 | Yes |
| proLevel1_ru_RU | string | - | 4000 | Yes |
| proLevel1_zh_CN | string | - | 4000 | Yes |
| proLevel1_zh_TW | string | - | 4000 | Yes |
| proLevel2_de_DE | string | - | 4000 | Yes |
| proLevel2_defaultValue | string | - | 4000 | Yes |
| proLevel2_en_GB | string | - | 4000 | Yes |
| proLevel2_en_US | string | - | 4000 | Yes |
| proLevel2_es_ES | string | - | 4000 | Yes |
| proLevel2_fr_FR | string | - | 4000 | Yes |
| proLevel2_ja_JP | string | - | 4000 | Yes |
| proLevel2_ko_KR | string | - | 4000 | Yes |
| proLevel2_localized | string | - | 4000 | Yes |
| proLevel2_nl_NL | string | - | 4000 | Yes |
| proLevel2_pt_BR | string | - | 4000 | Yes |
| proLevel2_pt_PT | string | - | 4000 | Yes |
| proLevel2_ru_RU | string | - | 4000 | Yes |
| proLevel2_zh_CN | string | - | 4000 | Yes |
| proLevel2_zh_TW | string | - | 4000 | Yes |
| proLevel3_de_DE | string | - | 4000 | Yes |
| proLevel3_defaultValue | string | - | 4000 | Yes |
| proLevel3_en_GB | string | - | 4000 | Yes |
| proLevel3_en_US | string | - | 4000 | Yes |
| proLevel3_es_ES | string | - | 4000 | Yes |
| proLevel3_fr_FR | string | - | 4000 | Yes |
| proLevel3_ja_JP | string | - | 4000 | Yes |
| proLevel3_ko_KR | string | - | 4000 | Yes |
| proLevel3_localized | string | - | 4000 | Yes |
| proLevel3_nl_NL | string | - | 4000 | Yes |
| proLevel3_pt_BR | string | - | 4000 | Yes |
| proLevel3_pt_PT | string | - | 4000 | Yes |
| proLevel3_ru_RU | string | - | 4000 | Yes |
| proLevel3_zh_CN | string | - | 4000 | Yes |
| proLevel3_zh_TW | string | - | 4000 | Yes |
| proLevel4_de_DE | string | - | 4000 | Yes |
| proLevel4_defaultValue | string | - | 4000 | Yes |
| proLevel4_en_GB | string | - | 4000 | Yes |
| proLevel4_en_US | string | - | 4000 | Yes |
| proLevel4_es_ES | string | - | 4000 | Yes |
| proLevel4_fr_FR | string | - | 4000 | Yes |
| proLevel4_ja_JP | string | - | 4000 | Yes |
| proLevel4_ko_KR | string | - | 4000 | Yes |
| proLevel4_localized | string | - | 4000 | Yes |
| proLevel4_nl_NL | string | - | 4000 | Yes |
| proLevel4_pt_BR | string | - | 4000 | Yes |
| proLevel4_pt_PT | string | - | 4000 | Yes |
| proLevel4_ru_RU | string | - | 4000 | Yes |
| proLevel4_zh_CN | string | - | 4000 | Yes |
| proLevel4_zh_TW | string | - | 4000 | Yes |
| proLevel5_de_DE | string | - | 4000 | Yes |
| proLevel5_defaultValue | string | - | 4000 | Yes |
| proLevel5_en_GB | string | - | 4000 | Yes |
| proLevel5_en_US | string | - | 4000 | Yes |
| proLevel5_es_ES | string | - | 4000 | Yes |
| proLevel5_fr_FR | string | - | 4000 | Yes |
| proLevel5_ja_JP | string | - | 4000 | Yes |
| proLevel5_ko_KR | string | - | 4000 | Yes |
| proLevel5_localized | string | - | 4000 | Yes |
| proLevel5_nl_NL | string | - | 4000 | Yes |
| proLevel5_pt_BR | string | - | 4000 | Yes |
| proLevel5_pt_PT | string | - | 4000 | Yes |
| proLevel5_ru_RU | string | - | 4000 | Yes |
| proLevel5_zh_CN | string | - | 4000 | Yes |
| proLevel5_zh_TW | string | - | 4000 | Yes |
| status | string | - | 255 | Yes |
| subModule | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

### SkillProfile

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | Yes |
| externalCode | string | - | 100 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| ratedSkills | Object | - | - | Yes |
| selfReportSkills | Object | - | - | Yes |
| status | string | - | 255 | Yes |
| transactionSequence | string | int64 | - | Yes |

## Object Primary Keys

Primary key fields identified for each entity:

- **BehaviorMappingEntity**: externalCode, mdfSystemRecordId, mdfSystemEntityId, CompetencyEntity_externalCode, mdfSystemVersionId, effectiveStartDate
- **CertificationContent**: externalCode, mdfSystemRecordId, sectionId, jobProfileId, JobProfile_externalCode, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **CertificationEntity**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **CompetencyContent**: externalCode, mdfSystemRecordId, sectionId, competencyMappingId, jobProfileId, JobProfile_externalCode, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **CompetencyEntity**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **CompetencyType**: externalCode
- **EmploymentConditionContent**: externalCode, mdfSystemRecordId, sectionId, jobProfileId, JobProfile_externalCode, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **EmploymentConditionEntity**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **FamilyCompetencyMappingEntity**: mdfSystemRecordId, externalCode, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate, FamilyEntity_externalCode
- **FamilyEntity**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **FamilySkillMappingEntity**: mdfSystemRecordId, externalCode, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate, FamilyEntity_externalCode
- **InterviewQuestionContent**: externalCode, mdfSystemRecordId, sectionId, jobProfileId, JobProfile_externalCode, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **InterviewQuestionEntity**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **JDTemplateFamilyMapping**: mdfSystemRecordId, externalCode, mdfSystemEntityId, mdfSystemVersionId, JobDescTemplate_externalCode, effectiveStartDate
- **JobCodeMappingEntity**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, RoleEntity_externalCode, effectiveStartDate
- **JobDescSection**: mdfSystemRecordId, externalCode, mdfSystemEntityId, mdfSystemVersionId, JobDescTemplate_externalCode, effectiveStartDate
- **JobDescTemplate**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **JobProfile**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **JobProfileLocalizedData**: externalCode, mdfSystemRecordId, sectionId, JobProfile_externalCode, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **JobResponsibilityContent**: externalCode, mdfSystemRecordId, sectionId, jobProfileId, JobProfile_externalCode, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **JobResponsibilityEntity**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **PhysicalReqContent**: externalCode, mdfSystemRecordId, sectionId, jobProfileId, JobProfile_externalCode, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **PhysicalReqEntity**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **PositionCompetencyMappingEntity**: externalCode, mdfSystemRecordId, PositionEntity_externalCode, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **PositionEntity**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **PositionSkillMappingEntity**: externalCode, mdfSystemRecordId, PositionEntity_externalCode, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **RatedSkillMapping**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, SkillProfile_externalCode, effectiveStartDate
- **RelevantIndustryContent**: externalCode, mdfSystemRecordId, sectionId, jobProfileId, JobProfile_externalCode, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **RelevantIndustryEntity**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **RoleCompetencyBehaviorMappingEntity**: externalCode, RoleEntity_externalCode
- **RoleCompetencyMappingEntity**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, RoleEntity_externalCode, effectiveStartDate
- **RoleEntity**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **RoleSkillMappingEntity**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, RoleEntity_externalCode, effectiveStartDate
- **RoleTalentPoolMappingEntity**: externalCode, mdfSystemRecordId, mdfSystemEntityId, talentPoolId, mdfSystemVersionId, RoleEntity_externalCode, effectiveStartDate
- **SelfReportSkillMapping**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, SkillProfile_externalCode, effectiveStartDate
- **SkillContent**: externalCode, mdfSystemRecordId, sectionId, jobProfileId, JobProfile_externalCode, mdfSystemEntityId, skillMappingId, mdfSystemVersionId, effectiveStartDate
- **SkillEntity**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **SkillProfile**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate

## Object's Ingestion Type

Ingestion types are determined based on API capabilities:


| Entity | Ingestion Type | Cursor Field |
|--------|---------------|--------------|
| BehaviorMappingEntity | cdc | lastModifiedDateTime |
| CertificationContent | cdc | lastModifiedDateTime |
| CertificationEntity | cdc | lastModifiedDateTime |
| CompetencyContent | cdc | lastModifiedDateTime |
| CompetencyEntity | cdc | lastModifiedDateTime |
| CompetencyType | cdc | lastModifiedDateTime |
| EmploymentConditionContent | cdc | lastModifiedDateTime |
| EmploymentConditionEntity | cdc | lastModifiedDateTime |
| FamilyCompetencyMappingEntity | cdc | lastModifiedDateTime |
| FamilyEntity | cdc | lastModifiedDateTime |
| FamilySkillMappingEntity | cdc | lastModifiedDateTime |
| InterviewQuestionContent | cdc | lastModifiedDateTime |
| InterviewQuestionEntity | cdc | lastModifiedDateTime |
| JDTemplateFamilyMapping | cdc | lastModifiedDateTime |
| JobCodeMappingEntity | cdc | lastModifiedDateTime |
| JobDescSection | cdc | lastModifiedDateTime |
| JobDescTemplate | cdc | lastModifiedDateTime |
| JobProfile | cdc | lastModifiedDateTime |
| JobProfileLocalizedData | cdc | lastModifiedDateTime |
| JobResponsibilityContent | cdc | lastModifiedDateTime |
| JobResponsibilityEntity | cdc | lastModifiedDateTime |
| PhysicalReqContent | cdc | lastModifiedDateTime |
| PhysicalReqEntity | cdc | lastModifiedDateTime |
| PositionCompetencyMappingEntity | cdc | lastModifiedDateTime |
| PositionEntity | cdc | lastModifiedDateTime |
| PositionSkillMappingEntity | cdc | lastModifiedDateTime |
| RatedSkillMapping | cdc | lastModifiedDateTime |
| RelevantIndustryContent | cdc | lastModifiedDateTime |
| RelevantIndustryEntity | cdc | lastModifiedDateTime |
| RoleCompetencyBehaviorMappingEntity | cdc | lastModifiedDateTime |
| RoleCompetencyMappingEntity | cdc | lastModifiedDateTime |
| RoleEntity | cdc | lastModifiedDateTime |
| RoleSkillMappingEntity | cdc | lastModifiedDateTime |
| RoleTalentPoolMappingEntity | cdc | lastModifiedDateTime |
| SelfReportSkillMapping | cdc | lastModifiedDateTime |
| SkillContent | cdc | lastModifiedDateTime |
| SkillEntity | cdc | lastModifiedDateTime |
| SkillProfile | cdc | lastModifiedDateTime |

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

**Get all records from CertificationContent:**
```http
GET https://{api-server}/odata/v2/CertificationContent
Authorization: Basic {base64_credentials}
Accept: application/json
```

**Get records modified after a date (for CDC):**
```http
GET https://{api-server}/odata/v2/CertificationContent?$filter=lastModifiedDateTime ge datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc
Authorization: Basic {base64_credentials}
Accept: application/json
```

**Get specific fields with pagination:**
```http
GET https://{api-server}/odata/v2/CertificationContent?$select=externalCode,createdDateTime,lastModifiedDateTime&$top=100&$skip=0&$inlinecount=allpages
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
          "uri": "https://{api-server}/odata/v2/CertificationContent('key')",
          "type": "SFOData.CertificationContent"
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

- **SAP SuccessFactors API Spec**: `ECSkillsManagement.json`
- **SAP Help Portal**: [Skills Management on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/f92ca967420c4596acec7640d0f00587.html)
- **SAP API Business Hub**: [SAP SuccessFactors APIs](https://api.sap.com/package/SuccessFactorsFoundation/overview)
- **Authentication Guide**: [SAP SuccessFactors OData API Authentication](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/5c8bca0af1654b05a83193b2922dcee2.html)
