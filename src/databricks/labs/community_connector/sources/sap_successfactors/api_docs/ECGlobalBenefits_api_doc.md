# Global Benefits API Documentation

## Authorization

- **Method**: Basic Authentication
- **Header**: `Authorization: Basic base64(username:password)`
- **Username format**: `username@companyId`

## Object List

The following entities/objects are available in this API:

- ACAReportingDependentDetails
- ACAReportingInformation
- Benefit
- Benefit Employee Life Event Declaration Form
- Benefit Pension Additional Contribution Limits
- Benefit Pension Additional Employee Contribution Detail
- Benefit Pension Min Max Contribution Limits
- BenefitAutomaticActionConfiguration
- BenefitBalanceCarryForward
- BenefitBalanceCarryForwardDetail
- BenefitClaimAccumulation
- BenefitCompanyCar
- BenefitCompanyCarAllowedModels
- BenefitCompanyCarClaim
- BenefitCompanyCarEnrollment
- BenefitCompanyCarLeaseServiceProvider
- BenefitCompanyCarRecommendedVendors
- BenefitCompanyHousing
- BenefitCompanyHousingEnrollment
- BenefitContact
- BenefitDeductibleAllowanceEnrollment
- BenefitDeductionDetails
- BenefitDependentDetail
- BenefitDocuments
- BenefitEffectiveDateConfiguration
- BenefitEmployeeClaim
- BenefitEmployeeClaimDetail
- BenefitEmployeeOptoutRequests
- BenefitEnrollment
- BenefitEnrollmentDependencyConfiguration
- BenefitEnrollmentDependencyDetails
- BenefitEnrollmentGroup
- BenefitEnrollmentOptoutDetails
- BenefitEvent
- BenefitExceptionDetails
- BenefitFuelReimbursement
- BenefitFuelReimbursementClaimDetail
- BenefitHSAEmployerContribution
- BenefitHSAEmployerContributionDetail
- BenefitHSAEmployerContributionTierDetail
- BenefitHyperlinkConfiguration
- BenefitInsuranceCoverage
- BenefitInsuranceCoverageDetails
- BenefitInsuranceCoverageOptions
- BenefitInsuranceDependentDetail
- BenefitInsuranceEnrolleeOptions
- BenefitInsuranceEnrolleeType
- BenefitInsurancePlan
- BenefitInsurancePlanEnrollmentDetails
- BenefitInsurancePlanUSA
- BenefitInsuranceProvider
- BenefitInsuranceRateChart
- BenefitInsuranceRateChartEnrollee
- BenefitInsuranceRateChartFixedAmount
- BenefitLeaveTravelReimbursementClaim
- BenefitLegalEntity
- BenefitLifeEventConfiguration
- BenefitOpenEnrollmentCycleConfiguration
- BenefitOverviewHyperlinkConfiguration
- BenefitOverviewHyperlinkDetails
- BenefitPaymentOptions
- BenefitPensionDependentNominees
- BenefitPensionEmployeeContributionDetail
- BenefitPensionEmployerContributionDetail
- BenefitPensionEnrollmentContributionDetail
- BenefitPensionFund
- BenefitPensionFundEnrollmentContributionDetail
- BenefitPensionNonDependentNominees
- BenefitPensionStatutoryMinimumLookup
- BenefitProgram
- BenefitProgramEnrollment
- BenefitProgramEnrollmentDetail
- BenefitProgramExceptionDetails
- BenefitSavingsPlanCatchUpDetail
- BenefitSavingsPlanContingentBeneficiary
- BenefitSavingsPlanERContributionConfig
- BenefitSavingsPlanERContributionConfigDetail
- BenefitSavingsPlanEnrollmentContributionDetail
- BenefitSavingsPlanEnrollmentDetails
- BenefitSavingsPlanPrimaryBeneficiary
- BenefitSavingsPlanSubType
- BenefitSavingsPlanSubTypeCountryLookup
- BenefitSavingsPlanTierConfiguration
- BenefitSchedulePeriod
- BenefitSchedules
- BenefitsConfigUIScreenLookup
- BenefitsConfirmationStatementConfiguration
- BenefitsException
- BenefitsIntegrationOneTimeInfo
- BenefitsIntegrationRecurringInfo
- Employee with Employer Match Contribution Entries
- Employee with Employer Match Contributions
- IRS Premium Table
- Imputed Cost for Age Ranges
- InsuranceBenefitDetails
- InsuranceEnrollmentFieldsConfiguration
- Life Event for Benefit
- Pension Banding Configuration
- Pension Banding Configuration Details
- SavingsAccountBenefitDetails
- SavingsAccountDeductionDetails
- SavingsAccountTierConfiguration
- SavingsAccountUSA

## Object Schema

Detailed schema for each entity:

### ACAReportingDependentDetails

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| ACAReportingInformation_externalCode | string | - | 128 | No |
| benEligibilityEndDate | string | - | - | Yes |
| benEligibilityStartDate | string | - | - | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| depDateOfBirth | string | - | - | Yes |
| dependentId | string | int64 | - | Yes |
| dependentName | string | - | 255 | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### ACAReportingInformation

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| benefit | string | - | 128 | Yes |
| benefitEligibilityEndDate | string | - | - | Yes |
| benefitNav | Object | - | - | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| dependentDetails | Object | - | - | Yes |
| enrollmentEffectiveFrom | string | - | - | Yes |
| enrollmentRequestDate | string | - | - | Yes |
| exceptionId | string | - | 128 | Yes |
| exceptionIdNav | Ref(BenefitsException) | - | - | No |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mecPlan | string | - | 128 | Yes |
| mecPlanNav | Object | - | - | Yes |
| minimumCostValue | string | decimal | - | Yes |
| offerDate | string | - | - | Yes |
| offerStatus | string | - | 128 | Yes |
| optOutDate | string | - | - | Yes |
| schedulePeriod | string | - | 128 | Yes |
| schedulePeriodNav | Ref(BenefitSchedulePeriod) | - | - | No |
| workerId | string | - | 100 | Yes |

### Benefit

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| ageOfRetirement | string | decimal | - | Yes |
| annualMaxContributionAmount | string | decimal | - | Yes |
| annualMinContributionAmount | string | decimal | - | Yes |
| balanceCarryForward | boolean | - | - | Yes |
| balanceCarryForwardOption | Ref(BenefitBalanceCarryForward) | - | - | No |
| bandingsConfigurationNav | Ref(PensionBandingConfiguration) | - | - | No |
| benCompanyCar | Ref(BenefitCompanyCar) | - | - | No |
| benPaymentOptions | Object | - | - | Yes |
| benefitCompanyHousing | Ref(BenefitCompanyHousing) | - | - | No |
| benefitContact | Object | - | - | Yes |
| benefitHyperlinkConfiguration | Object | - | - | Yes |
| benefitId | string | - | 128 | No |
| benefitName | string | - | 128 | Yes |
| benefitProgram | string | - | 128 | Yes |
| benefitProgramNav | Ref(BenefitProgram) | - | - | No |
| benefitSavingsPlanSubType | string | - | 128 | Yes |
| benefitSavingsPlanSubTypeNav | Ref(BenefitSavingsPlanSubType) | - | - | No |
| benefitSavingsPlanTierConfig | Object | - | - | Yes |
| benefitSchedule | string | - | 128 | Yes |
| benefitScheduleNav | Ref(BenefitSchedules) | - | - | No |
| benefitShortDescription | string | - | 255 | Yes |
| benefitSpecific | string | - | 128 | Yes |
| benefitType | string | - | 255 | Yes |
| bothBalanceCarryForwardParametersPresent | boolean | - | - | Yes |
| carryForwardEnrollment | boolean | - | - | Yes |
| claim | string | - | 128 | Yes |
| claimDetail | string | - | 128 | Yes |
| claimDetailRequired | boolean | - | - | Yes |
| claimScreenID | string | - | 255 | Yes |
| claimsLimitPerFrequencyPeriod | string | int64 | - | Yes |
| conversionFactor | string | decimal | - | Yes |
| country | string | - | 128 | Yes |
| coverage | string | - | 128 | Yes |
| coverageNav | Ref(BenefitInsuranceCoverage) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| creditPoints | string | decimal | - | Yes |
| currency | string | - | 128 | Yes |
| decimalPrecisionSettingForContributionAmount | boolean | - | - | Yes |
| deductionDetails | Object | - | - | Yes |
| deductionStartDate | string | - | 255 | Yes |
| dependentSpecificRule | boolean | - | - | Yes |
| dummyField | string | - | 255 | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| effectiveStatus | string | - | 255 | Yes |
| eligibilityRuleId | string | - | 128 | Yes |
| eligibleBenefits | Object | - | - | Yes |
| emailNotificationForEnrollment | boolean | - | - | Yes |
| employeeClaimWorkflowId | string | - | 32 | Yes |
| employeeContributionDetail | Ref(BenefitPensionEmployeeContributionDetail) | - | - | No |
| employeeEnrollmentEditAllowed | boolean | - | - | Yes |
| employeeEnrollmentEditType | string | - | 255 | Yes |
| employeeEnrollmentWorkflowId | string | - | 32 | Yes |
| employeeWithEmployerMatchContributions | string | - | 128 | Yes |
| employeeWithEmployerMatchContributionsNav | Ref(EmployeeWithEmployerMatchContributions) | - | - | No |
| employerContributionDetail | Ref(BenefitPensionEmployerContributionDetail) | - | - | No |
| employerContributionHSANav | Ref(BenefitHSAEmployerContribution) | - | - | No |
| enrolleeOptions | string | - | 128 | Yes |
| enrolleeOptionsNav | Ref(BenefitInsuranceEnrolleeOptions) | - | - | No |
| enrollment | string | - | 128 | Yes |
| enrollmentRequired | boolean | - | - | Yes |
| enrollmentScreenID | string | - | 255 | Yes |
| enrollmentType | string | - | 255 | Yes |
| entitlementAmount | string | decimal | - | Yes |
| exceedEntitlementAmount | boolean | - | - | Yes |
| exceptionWorkflowId | string | - | 32 | Yes |
| forms | Object | - | - | Yes |
| frequency | string | - | 32 | Yes |
| insurancePlans | Object | - | - | Yes |
| insuranceType | string | - | 128 | Yes |
| jobEnrollmentEditAllowed | boolean | - | - | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| legalEntities | Object | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| multipleFundSelectionsAllowed | boolean | - | - | Yes |
| noOfClaimTansactions | string | int64 | - | Yes |
| noOfDependentsToConsider | string | int64 | - | Yes |
| nomineeRelevant | boolean | - | - | Yes |
| payrollIntegration | boolean | - | - | Yes |
| pensionAdditionalEmployeeContributionDetail | Ref(BenefitPensionAdditionalEmployeeContributionDetail) | - | - | No |
| pensionFunds | Object | - | - | Yes |
| pensionMinMaxContributionLimitsNav | Ref(BenefitPensionMinMaxContributionLimits) | - | - | No |
| plan | string | - | 128 | Yes |
| planNav | Ref(BenefitInsurancePlan) | - | - | No |
| policyDocuments | Object | - | - | Yes |
| savingsPlanCatchUpDetailNav | Ref(BenefitSavingsPlanCatchUpDetail) | - | - | No |
| savingsPlanSubType | string | - | 128 | Yes |
| status | string | - | 255 | Yes |
| statutoryMinimumLookUp | Object | - | - | Yes |
| supressClientDateValidation | boolean | - | - | Yes |
| triggerDate | string | - | 255 | Yes |
| typeOfPension | string | - | 255 | Yes |
| usefulLinks | Object | - | - | Yes |
| walletType | string | - | 255 | Yes |
| walletsAssociated | Object | - | - | Yes |

### BenefitAutomaticActionConfiguration

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitLifeEventConfiguration_configurationId | string | - | 128 | No |
| BenefitLifeEventConfiguration_effectiveStartDate | string | - | - | No |
| actionFor | string | - | 128 | Yes |
| benefit | string | - | 128 | Yes |
| benefitNav | Object | - | - | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| deductionEffectiveDateRule | string | - | 128 | Yes |
| effectiveDateRule | string | - | 128 | Yes |
| id | string | - | 128 | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### BenefitBalanceCarryForward

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| Benefit_benefitId | string | - | 128 | No |
| Benefit_effectiveStartDate | string | - | - | No |
| balanceCarryForwardUptoNoOfSchedulePeriods | string | int64 | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| maximumBalanceCarryForwardAmount | string | decimal | - | Yes |
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
| upperLimitOnTotalBalanceCarryForwardAmount | string | decimal | - | Yes |

### BenefitBalanceCarryForwardDetail

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitClaimAccumulation_externalCode | string | int64 | - | No |
| balanceCarryForwardAmount | string | decimal | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
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
| schedulePeriod | string | - | 128 | Yes |
| schedulePeriodNav | Ref(BenefitSchedulePeriod) | - | - | No |

### BenefitClaimAccumulation

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| accumulatedAmount | string | decimal | - | Yes |
| accumulatedCredits | string | decimal | - | Yes |
| balanceCarryForwardAmount | string | decimal | - | Yes |
| balanceCredits | string | decimal | - | Yes |
| benefit | string | - | 128 | Yes |
| benefitBalanceCarryForwardDetails | Object | - | - | Yes |
| benefitClaims | Object | - | - | Yes |
| benefitEnrollments | Object | - | - | Yes |
| benefitNav | Object | - | - | Yes |
| claimWindowEnd | string | - | - | Yes |
| claimWindowStart | string | - | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| currency | string | - | 128 | Yes |
| exception | string | - | 128 | Yes |
| exceptionNav | Ref(BenefitsException) | - | - | No |
| externalCode | string | int64 | - | No |
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
| remainingAmount | string | decimal | - | Yes |
| schedulePeriod | string | - | 128 | Yes |
| schedulePeriodNav | Ref(BenefitSchedulePeriod) | - | - | No |
| workerId | string | - | 100 | Yes |

### BenefitCompanyCar

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| Benefit_benefitId | string | - | 128 | No |
| Benefit_effectiveStartDate | string | - | - | No |
| benCompanyCarAllowedModels | Object | - | - | Yes |
| benCompanyCarRecommendedVendors | Object | - | - | Yes |
| benefitCompanyCarId | string | int64 | - | No |
| buyBackSupported | string | - | 128 | Yes |
| carLeaseServiceProviders | Object | - | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveStatus | string | - | 255 | Yes |
| installmentFrequency | string | - | 32 | Yes |
| interestRate | string | decimal | - | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| lockInPolicyDuration | string | int64 | - | Yes |
| maintainanceCovered | string | - | 255 | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| roadTaxCovered | string | - | 255 | Yes |

### BenefitCompanyCarAllowedModels

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| carModelName | string | - | 128 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| externalCode | string | - | 128 | No |
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

### BenefitCompanyCarClaim

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEmployeeClaim_id | string | int64 | - | No |
| carModel | string | - | 128 | Yes |
| carValue | string | decimal | - | Yes |
| carVendor | string | - | 128 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| currency | string | - | 128 | Yes |
| id | string | int64 | - | No |
| installmentFrequency | string | int64 | - | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| leaseServiceProvider | string | - | 128 | Yes |
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
| policyDuration | string | int64 | - | Yes |
| unitOfPeriod | string | - | 255 | Yes |

### BenefitCompanyCarEnrollment

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEnrollment_effectiveStartDate | string | - | - | No |
| BenefitEnrollment_id | string | int64 | - | No |
| benefitID | string | - | 255 | Yes |
| carLeaseEndDate | string | - | - | Yes |
| carLeaseServiceProvider | string | - | 128 | Yes |
| carLeaseServiceProviderNav | Object | - | - | Yes |
| carLeaseStartDate | string | - | - | Yes |
| carModel | string | - | 128 | Yes |
| carModelNav | Object | - | - | Yes |
| carRegistrationNumber | string | - | 255 | Yes |
| carVendor | string | - | 128 | Yes |
| carVendorNav | Object | - | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| currency | string | - | 128 | Yes |
| effectiveStatus | string | - | 255 | Yes |
| emiAmount | string | decimal | - | Yes |
| employeeContribution | string | decimal | - | Yes |
| employerContribution | string | decimal | - | Yes |
| exShowroomValue | string | decimal | - | Yes |
| id | string | int64 | - | No |
| installmentFrequency | string | - | 32 | Yes |
| interestRate | string | decimal | - | Yes |
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
| periodDuration | string | int64 | - | Yes |
| unitOfPeriod | string | - | 255 | Yes |

### BenefitCompanyCarLeaseServiceProvider

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| emiInterestRate | string | decimal | - | Yes |
| externalCode | string | - | 128 | No |
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
| serviceProviderName | string | - | 128 | Yes |

### BenefitCompanyCarRecommendedVendors

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| carVendorName | string | - | 128 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| externalCode | string | - | 128 | No |
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

### BenefitCompanyHousing

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| Benefit_benefitId | string | - | 128 | No |
| Benefit_effectiveStartDate | string | - | - | No |
| accommodationType | string | - | 128 | Yes |
| cityCategory | string | - | 128 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| id | string | int64 | - | No |
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

### BenefitCompanyHousingEnrollment

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEnrollment_effectiveStartDate | string | - | - | No |
| BenefitEnrollment_id | string | int64 | - | No |
| accommodationType | string | - | 128 | Yes |
| address | string | - | 255 | Yes |
| cityCategory | string | - | 128 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| furnitureCost | string | decimal | - | Yes |
| helperCost | string | decimal | - | Yes |
| id | string | int64 | - | No |
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
| name | string | - | 255 | Yes |
| personalId | string | - | 255 | Yes |
| personalIdType | string | - | 128 | Yes |

### BenefitContact

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| contactEmail | string | - | 255 | Yes |
| contactEmployeeId | string | - | 100 | Yes |
| contactPhone | string | - | 255 | Yes |
| contactType | string | - | 128 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| employeeName | string | - | 128 | Yes |
| externalCode | string | - | 128 | No |
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

### BenefitDeductibleAllowanceEnrollment

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEnrollment_effectiveStartDate | string | - | - | No |
| BenefitEnrollment_id | string | - | 38 | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| employeeContribution | string | decimal | - | Yes |
| employeeContributionPayComponent | string | - | 32 | Yes |
| employerContribution | string | decimal | - | Yes |
| employerContributionPayComponent | string | - | 32 | Yes |
| id | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### BenefitDeductionDetails

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| Benefit_benefitId | string | - | 128 | No |
| Benefit_effectiveStartDate | string | - | - | No |
| basePayComponent | string | - | 32 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| dedcutionDetailId | string | int64 | - | No |
| deductionPayComponent | string | - | 32 | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| payFrequency | string | - | 32 | Yes |

### BenefitDependentDetail

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEmployeeClaim_id | string | int64 | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| dateOfBirth | string | - | - | Yes |
| dependentName | string | - | 128 | No |
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
| relationShipType | string | - | 128 | Yes |

### BenefitDocuments

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| id | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemCreatedBy | string | - | 255 | Yes |
| mdfSystemCreatedDate | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
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
| shortName | string | - | 128 | Yes |
| usefulLink | string | - | 255 | Yes |

### BenefitEffectiveDateConfiguration

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitLifeEventConfiguration_configurationId | string | - | 128 | No |
| BenefitLifeEventConfiguration_effectiveStartDate | string | - | - | No |
| benefit | string | - | 128 | No |
| benefitNav | Object | - | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| deductionRule | string | - | 128 | Yes |
| exceptionFor | string | - | 128 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| rule | string | - | 128 | Yes |

### BenefitEmployeeClaim

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| benFuelReimbursement | Ref(BenefitFuelReimbursement) | - | - | No |
| benTravelReimbursementClaim | Ref(BenefitLeaveTravelReimbursementClaim) | - | - | No |
| benefit | string | - | 128 | Yes |
| benefitContacts | Object | - | - | Yes |
| benefitDataSource | string | - | 255 | Yes |
| benefitDataSourceWithExternalCode | string | - | 255 | Yes |
| benefitDependentDetail | Object | - | - | Yes |
| benefitEmployeeCarClaim | Ref(BenefitCompanyCarClaim) | - | - | No |
| benefitEmployeeClaimDetail | Object | - | - | Yes |
| benefitFuelReimbursementClaimDetail | Object | - | - | Yes |
| benefitNav | Object | - | - | Yes |
| benefitProgram | string | - | 128 | Yes |
| benefitProgramNav | Object | - | - | Yes |
| byPassWorkflow | boolean | - | - | Yes |
| claimDate | string | - | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| currency | string | - | 128 | Yes |
| entitlementAmount | string | decimal | - | Yes |
| exception | string | - | 128 | Yes |
| exceptionNav | Ref(BenefitsException) | - | - | No |
| externalName | string | - | 128 | Yes |
| forms | Object | - | - | Yes |
| id | string | int64 | - | No |
| isTotalAmountReadOnly | string | - | 255 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemCreatedBy | string | - | 255 | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| nrpId | string | int64 | - | Yes |
| policyDocuments | Object | - | - | Yes |
| recordStatus | string | - | 255 | Yes |
| remarks | string | - | 1024 | Yes |
| status | string | - | 255 | Yes |
| totalAmount | string | decimal | - | Yes |
| usefulLinks | Object | - | - | Yes |
| workerId | string | - | 100 | Yes |

### BenefitEmployeeClaimDetail

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEmployeeClaim_id | string | int64 | - | No |
| amount | string | decimal | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| currency | string | - | 128 | Yes |
| cust_patientName | string | - | 100 | Yes |
| cust_receiptDate | string | - | - | Yes |
| cust_receiptDate2 | string | - | - | Yes |
| cust_receiptNo | string | - | 50 | Yes |
| cust_relationship | string | - | 128 | Yes |
| description | string | - | 255 | Yes |
| id | string | int64 | - | No |
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

### BenefitEmployeeLifeEventDeclarationForm

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| employeeLifeEventId | string | - | 128 | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lifeEventDate | string | - | - | Yes |
| lifeEventId | string | - | 128 | Yes |
| lifeEventIdNav | Ref(LifeEventForBenefit) | - | - | No |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| workerId | string | - | 100 | Yes |

### BenefitEmployeeOptoutRequests

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| benefitEnrollmentOptoutDetails | Object | - | - | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| workerId | string | - | 100 | No |

### BenefitEnrollment

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| amount | string | decimal | - | Yes |
| amountFromWallet | string | decimal | - | Yes |
| benefit | string | - | 128 | Yes |
| benefitCompanyCarEnrollment | Ref(BenefitCompanyCarEnrollment) | - | - | No |
| benefitCompanyHousingEnrollment | Ref(BenefitCompanyHousingEnrollment) | - | - | No |
| benefitContacts | Object | - | - | Yes |
| benefitDataSource | string | - | 255 | Yes |
| benefitDataSourceWithExternalCode | string | - | 255 | Yes |
| benefitDeductibleAllowanceEnrollment | Ref(BenefitDeductibleAllowanceEnrollment) | - | - | No |
| benefitEntitlementAmount | string | decimal | - | Yes |
| benefitInsurancePlanEnrollmentDetails | Ref(BenefitInsurancePlanEnrollmentDetails) | - | - | No |
| benefitNav | Ref(Benefit) | - | - | No |
| benefitPaymentOption | string | - | 128 | Yes |
| benefitPaymentOptionNav | Ref(BenefitPaymentOptions) | - | - | No |
| benefitPensionDependentNominees | Object | - | - | Yes |
| benefitPensionEnrollmentContributionDetail | Ref(BenefitPensionEnrollmentContributionDetail) | - | - | No |
| benefitPensionFundEnrollmentContributionDetail | Object | - | - | Yes |
| benefitPensionNonDependentNominees | Object | - | - | Yes |
| benefitProgram | string | - | 128 | Yes |
| benefitProgramNav | Ref(BenefitProgram) | - | - | No |
| benefitSavingsPlanContingentBeneficiaries | Object | - | - | Yes |
| benefitSavingsPlanEmployerContributionNav | Ref(BenefitSavingsPlanERContributionConfig) | - | - | No |
| benefitSavingsPlanEnrollmentContributionDetail | Object | - | - | Yes |
| benefitSavingsPlanEnrollmentDetails | Ref(BenefitSavingsPlanEnrollmentDetails) | - | - | No |
| benefitSavingsPlanPrimaryBeneficiaries | Object | - | - | Yes |
| compensationAdjustmentUntil | string | - | - | Yes |
| compensationId | string | int64 | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| creditPointsFromWallet | string | decimal | - | Yes |
| currency | string | - | 128 | Yes |
| deductionStartDate | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| effectiveStatus | string | - | 255 | Yes |
| eligibleWallet | string | - | 128 | Yes |
| eligibleWalletAmount | string | decimal | - | Yes |
| eligibleWalletCredits | string | decimal | - | Yes |
| eligibleWalletNav | Ref(Benefit) | - | - | No |
| eligibleWalletWithDataSource | string | - | 255 | Yes |
| enrollmentContext | string | - | 255 | Yes |
| enrollmentDate | string | - | - | Yes |
| exception | string | - | 128 | Yes |
| exceptionNav | Ref(BenefitsException) | - | - | No |
| externalName | string | - | 128 | Yes |
| forms | Object | - | - | Yes |
| id | string | int64 | - | No |
| jobRunDate | string | - | - | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| policyDocuments | Object | - | - | Yes |
| previousEnrollmentId | string | int64 | - | Yes |
| recordStatus | string | - | 255 | Yes |
| retirementDate | string | - | - | Yes |
| schedulePeriod | string | - | 128 | Yes |
| schedulePeriodNav | Ref(BenefitSchedulePeriod) | - | - | No |
| usefulLinks | Object | - | - | Yes |
| walletConsumedTill | string | - | - | Yes |
| workerId | string | - | 100 | Yes |

### BenefitEnrollmentDependencyConfiguration

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| benefitDependencyId | string | - | 128 | No |
| benefitEnrollmentDependencyDetails | Object | - | - | Yes |
| comment_de_DE | string | - | 50 | Yes |
| comment_defaultValue | string | - | 50 | Yes |
| comment_en_DEBUG | string | - | 50 | Yes |
| comment_en_GB | string | - | 50 | Yes |
| comment_en_US | string | - | 50 | Yes |
| comment_es_ES | string | - | 50 | Yes |
| comment_fr_FR | string | - | 50 | Yes |
| comment_ja_JP | string | - | 50 | Yes |
| comment_ko_KR | string | - | 50 | Yes |
| comment_localized | string | - | 50 | Yes |
| comment_nl_NL | string | - | 50 | Yes |
| comment_pt_BR | string | - | 50 | Yes |
| comment_pt_PT | string | - | 50 | Yes |
| comment_ru_RU | string | - | 50 | Yes |
| comment_zh_CN | string | - | 50 | Yes |
| comment_zh_TW | string | - | 50 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### BenefitEnrollmentDependencyDetails

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEnrollmentDependencyConfiguration_benefitDependencyId | string | - | 128 | No |
| BenefitEnrollmentDependencyConfiguration_effectiveStartDate | string | - | - | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| dependentBenefit | string | - | 128 | Yes |
| dependentBenefitNav | Object | - | - | Yes |
| dependentInsurancePlan | string | - | 128 | Yes |
| dependentInsurancePlanNav | Object | - | - | Yes |
| enrollmentCondition | string | - | 128 | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| leadBenefit | string | - | 128 | Yes |
| leadBenefitNav | Object | - | - | Yes |
| leadInsurancePlan | string | - | 128 | Yes |
| leadInsurancePlanNav | Object | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### BenefitEnrollmentGroup

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| benefitEnrollments | Object | - | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| id | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| openEnrollmentConfig | string | - | 128 | Yes |
| openEnrollmentConfigNav | Ref(BenefitOpenEnrollmentCycleConfiguration) | - | - | No |
| recordStatus | string | - | 255 | Yes |
| schedulePeriod | string | - | 128 | Yes |
| schedulePeriodNav | Ref(BenefitSchedulePeriod) | - | - | No |
| workerId | string | - | 100 | Yes |

### BenefitEnrollmentOptoutDetails

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEmployeeOptoutRequests_workerId | string | - | 100 | No |
| benefitEnrollment | string | - | 128 | Yes |
| benefitEnrollmentId | string | - | 255 | Yes |
| benefitEnrollmentNav | Object | - | - | Yes |
| benefitSchedulePeriod | string | - | 128 | Yes |
| benefitSchedulePeriodNav | Ref(BenefitSchedulePeriod) | - | - | No |
| contributionChangeDate | string | - | - | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| enrollmentChangeDate | string | - | - | Yes |
| id | string | - | 128 | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| optedoutBy | string | - | 255 | Yes |
| optoutRequestDate | string | - | - | Yes |

### BenefitEvent

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| eventCode | string | - | 128 | No |
| eventId | string | - | 128 | Yes |
| eventName_de_DE | string | - | 255 | Yes |
| eventName_defaultValue | string | - | 255 | Yes |
| eventName_en_GB | string | - | 255 | Yes |
| eventName_en_US | string | - | 255 | Yes |
| eventName_es_ES | string | - | 255 | Yes |
| eventName_fr_FR | string | - | 255 | Yes |
| eventName_ja_JP | string | - | 255 | Yes |
| eventName_ko_KR | string | - | 255 | Yes |
| eventName_localized | string | - | 255 | Yes |
| eventName_nl_NL | string | - | 255 | Yes |
| eventName_pt_BR | string | - | 255 | Yes |
| eventName_pt_PT | string | - | 255 | Yes |
| eventName_ru_RU | string | - | 255 | Yes |
| eventName_zh_CN | string | - | 255 | Yes |
| eventName_zh_TW | string | - | 255 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### BenefitExceptionDetails

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitsException_exceptionId | string | int64 | - | No |
| adjustmentAmount | string | decimal | - | Yes |
| benefit | string | - | 128 | No |
| benefitNav | Object | - | - | Yes |
| benefitSchedulePeriodDataSourceWithExternalCode | string | - | 255 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| deductionStartDateRule | string | - | 128 | Yes |
| effectiveStartDateRule | string | - | 128 | Yes |
| enrollmentEffectiveDate | string | - | - | Yes |
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
| schedulePeriod | string | - | 128 | Yes |
| schedulePeriodNav | Ref(BenefitSchedulePeriod) | - | - | No |

### BenefitFuelReimbursement

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEmployeeClaim_id | string | int64 | - | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveStatus | string | - | 255 | Yes |
| externalCode | string | - | 128 | No |
| fuelStationName | string | - | 255 | Yes |
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

### BenefitFuelReimbursementClaimDetail

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEmployeeClaim_id | string | int64 | - | No |
| amount | string | decimal | - | Yes |
| billNo | string | - | 255 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| fuelStationName | string | - | 255 | Yes |
| id | string | int64 | - | No |
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

### BenefitHSAEmployerContribution

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| employerContributionDetail | Object | - | - | Yes |
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

### BenefitHSAEmployerContributionDetail

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitHSAEmployerContribution_effectiveStartDate | string | - | - | No |
| BenefitHSAEmployerContribution_externalCode | string | - | 128 | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| employerContributionTierDetail | Object | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| paymentDateDay | string | - | 128 | Yes |
| paymentDateMonth | string | - | 128 | Yes |
| qualifyingEndDateDay | string | - | 128 | Yes |
| qualifyingEndDateMonth | string | - | 128 | Yes |
| qualifyingStartDateDay | string | - | 128 | Yes |
| qualifyingStartDateMonth | string | - | 128 | Yes |

### BenefitHSAEmployerContributionTierDetail

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitHSAEmployerContributionDetail_externalCode | string | int64 | - | No |
| BenefitHSAEmployerContribution_effectiveStartDate | string | - | - | No |
| BenefitHSAEmployerContribution_externalCode | string | - | 128 | No |
| coverageTier | string | - | 128 | No |
| coverageTierNav | Ref(BenefitInsuranceEnrolleeOptions) | - | - | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| employerContributionAmount | string | decimal | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### BenefitHyperlinkConfiguration

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| Benefit_benefitId | string | - | 128 | No |
| Benefit_effectiveStartDate | string | - | - | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| id | string | - | 128 | No |
| label_de_DE | string | - | 70 | Yes |
| label_defaultValue | string | - | 70 | Yes |
| label_en_DEBUG | string | - | 70 | Yes |
| label_en_GB | string | - | 70 | Yes |
| label_en_US | string | - | 70 | Yes |
| label_es_ES | string | - | 70 | Yes |
| label_fr_FR | string | - | 70 | Yes |
| label_ja_JP | string | - | 70 | Yes |
| label_ko_KR | string | - | 70 | Yes |
| label_localized | string | - | 70 | Yes |
| label_nl_NL | string | - | 70 | Yes |
| label_pt_BR | string | - | 70 | Yes |
| label_pt_PT | string | - | 70 | Yes |
| label_ru_RU | string | - | 70 | Yes |
| label_zh_CN | string | - | 70 | Yes |
| label_zh_TW | string | - | 70 | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| url | string | - | 255 | Yes |

### BenefitInsuranceCoverage

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| amount | string | decimal | - | Yes |
| basePayComponentGroup | string | - | 32 | Yes |
| benefitSalaryAmount | string | decimal | - | Yes |
| benefitSalaryCalculationRule | string | - | 128 | Yes |
| coverageId | string | - | 128 | No |
| coverageLevel | string | - | 128 | Yes |
| coverageName_de_DE | string | - | 255 | Yes |
| coverageName_defaultValue | string | - | 255 | Yes |
| coverageName_en_GB | string | - | 255 | Yes |
| coverageName_en_US | string | - | 255 | Yes |
| coverageName_es_ES | string | - | 255 | Yes |
| coverageName_fr_FR | string | - | 255 | Yes |
| coverageName_ja_JP | string | - | 255 | Yes |
| coverageName_ko_KR | string | - | 255 | Yes |
| coverageName_localized | string | - | 255 | Yes |
| coverageName_nl_NL | string | - | 255 | Yes |
| coverageName_pt_BR | string | - | 255 | Yes |
| coverageName_pt_PT | string | - | 255 | Yes |
| coverageName_ru_RU | string | - | 255 | Yes |
| coverageName_zh_CN | string | - | 255 | Yes |
| coverageName_zh_TW | string | - | 255 | Yes |
| coverageRoundingRule | string | - | 128 | Yes |
| coverageType | string | - | 255 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| factor | string | decimal | - | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| maximumCoverageAmount | string | decimal | - | Yes |
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
| minimumCoverageAmount | string | decimal | - | Yes |
| paycomponent | string | - | 32 | Yes |
| percentage | string | decimal | - | Yes |
| roundedCoverageAmount | string | decimal | - | Yes |
| toCoverageDetails | Object | - | - | Yes |

### BenefitInsuranceCoverageDetails

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitInsuranceCoverageOptions_externalCode | string | int64 | - | No |
| BenefitInsurancePlan_effectiveStartDate | string | - | - | No |
| BenefitInsurancePlan_id | string | - | 128 | No |
| coverage | string | - | 128 | Yes |
| coverageNav | Ref(BenefitInsuranceCoverage) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
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
| rateChart | string | - | 128 | Yes |
| rateChartNav | Object | - | - | Yes |

### BenefitInsuranceCoverageOptions

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitInsurancePlan_effectiveStartDate | string | - | - | No |
| BenefitInsurancePlan_id | string | - | 128 | No |
| coverageDetails | Object | - | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| enrolleeOptions | string | - | 128 | Yes |
| enrolleeOptionsNav | Ref(BenefitInsuranceEnrolleeOptions) | - | - | No |
| externalCode | string | int64 | - | No |
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

### BenefitInsuranceDependentDetail

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEnrollment_effectiveStartDate | string | - | - | No |
| BenefitEnrollment_id | string | int64 | - | No |
| BenefitInsurancePlanEnrollmentDetails_externalCode | string | int64 | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| dateOfBirth | string | - | - | Yes |
| dependentName | string | - | 128 | No |
| gender | string | - | 255 | Yes |
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
| relationShipType | string | - | 128 | Yes |
| smoking | string | - | 255 | Yes |

### BenefitInsuranceEnrolleeOptions

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| depEligibilityRule | string | - | 128 | Yes |
| dependentOption | string | - | 128 | Yes |
| enrolleType | Object | - | - | Yes |
| enrolleeOptionsName | string | - | 128 | Yes |
| id | string | - | 128 | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| maxTotalDependents | string | int64 | - | Yes |
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
| minTotalDependents | string | int64 | - | Yes |
| toCoverageOptions | Object | - | - | Yes |

### BenefitInsuranceEnrolleeType

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| maxDependents | string | int64 | - | Yes |
| mdfSystemCreatedBy | string | - | 255 | Yes |
| mdfSystemCreatedDate | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | No |
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
| minDependents | string | int64 | - | Yes |
| relationShipType | string | - | 128 | No |

### BenefitInsurancePlan

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| country | string | - | 128 | Yes |
| coverageOptions | Object | - | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| eligibilityRuleForCoverage | string | - | 128 | Yes |
| employeeContribution | string | - | 32 | Yes |
| employerContribution | string | - | 32 | Yes |
| frequency | string | - | 32 | Yes |
| id | string | - | 128 | No |
| insurancePlanUSA | Ref(BenefitInsurancePlanUSA) | - | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemCreatedBy | string | - | 255 | Yes |
| mdfSystemCreatedDate | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
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
| planName_de_DE | string | - | 255 | Yes |
| planName_defaultValue | string | - | 255 | Yes |
| planName_en_GB | string | - | 255 | Yes |
| planName_en_US | string | - | 255 | Yes |
| planName_es_ES | string | - | 255 | Yes |
| planName_fr_FR | string | - | 255 | Yes |
| planName_ja_JP | string | - | 255 | Yes |
| planName_ko_KR | string | - | 255 | Yes |
| planName_localized | string | - | 255 | Yes |
| planName_nl_NL | string | - | 255 | Yes |
| planName_pt_BR | string | - | 255 | Yes |
| planName_pt_PT | string | - | 255 | Yes |
| planName_ru_RU | string | - | 255 | Yes |
| planName_zh_CN | string | - | 255 | Yes |
| planName_zh_TW | string | - | 255 | Yes |
| premiumType | string | - | 255 | Yes |
| provider | string | - | 128 | Yes |
| providerNav | Ref(BenefitInsuranceProvider) | - | - | No |

### BenefitInsurancePlanEnrollmentDetails

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEnrollment_effectiveStartDate | string | - | - | No |
| BenefitEnrollment_id | string | int64 | - | No |
| benefitInsuranceDependentDetails | Object | - | - | Yes |
| benefitSalaryAmount | string | decimal | - | Yes |
| coverage | string | - | 128 | Yes |
| coverageNav | Ref(BenefitInsuranceCoverage) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| employeeContribution | string | decimal | - | Yes |
| employerContribution | string | decimal | - | Yes |
| enrolleeOptions | string | - | 128 | Yes |
| enrolleeOptionsNav | Ref(BenefitInsuranceEnrolleeOptions) | - | - | No |
| externalCode | string | int64 | - | No |
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
| plan | string | - | 128 | Yes |
| planNav | Object | - | - | Yes |
| provider | string | - | 128 | Yes |
| providerNav | Ref(BenefitInsuranceProvider) | - | - | No |
| roundedCoverageAmount | string | decimal | - | Yes |
| smoking | string | - | 255 | Yes |

### BenefitInsurancePlanUSA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitInsurancePlan_effectiveStartDate | string | - | - | No |
| BenefitInsurancePlan_id | string | - | 128 | No |
| IRSPremiumTable | string | - | 128 | Yes |
| IRSPremiumTableNav | Object | - | - | Yes |
| cobraRelevant | boolean | - | - | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| employeeContributionTaxAllocation | string | - | 128 | Yes |
| employeePostTaxContributionPayComponent | string | - | 32 | Yes |
| employeePreTaxContributionPayComponent | string | - | 32 | Yes |
| id | string | int64 | - | No |
| imputedIncomeHandling | string | - | 128 | Yes |
| imputedIncomePayComponent | string | - | 32 | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### BenefitInsuranceProvider

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| contactEmail | string | - | 255 | Yes |
| contactPerson | string | - | 255 | Yes |
| contactPhone | string | - | 255 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
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
| policyDocuments | Object | - | - | Yes |
| providerId | string | - | 128 | No |
| providerName | string | - | 128 | Yes |
| usefulLinks | Object | - | - | Yes |

### BenefitInsuranceRateChart

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| ageAsOfDay | string | - | 255 | Yes |
| ageAsOfMonth | string | - | 255 | Yes |
| ageAsOfYear | string | - | 255 | Yes |
| coverage | string | - | 128 | Yes |
| coverageNav | Ref(BenefitInsuranceCoverage) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| currency | string | - | 128 | Yes |
| effectiveStartDate | string | - | - | No |
| factor | string | decimal | - | Yes |
| genderRelevant | boolean | - | - | Yes |
| insurancePlan | string | - | 128 | Yes |
| insurancePlanNav | Ref(BenefitInsurancePlan) | - | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemCreatedBy | string | - | 255 | Yes |
| mdfSystemCreatedDate | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
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
| provider | string | - | 128 | Yes |
| providerNav | Ref(BenefitInsuranceProvider) | - | - | No |
| rateChartEnrollee | Object | - | - | Yes |
| rateChartFixedAmount | Object | - | - | Yes |
| rateChartId | string | - | 128 | No |

### BenefitInsuranceRateChartEnrollee

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitInsuranceRateChart_effectiveStartDate | string | - | - | No |
| BenefitInsuranceRateChart_rateChartId | string | - | 128 | No |
| ageFrom | string | int64 | - | Yes |
| ageTo | string | int64 | - | Yes |
| benefitInsuranceEnrolleeType | string | - | 128 | Yes |
| benefitInsuranceEnrolleeTypeNav | Object | - | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| employeeContribution | string | decimal | - | Yes |
| employerContribution | string | decimal | - | Yes |
| externalCode | string | int64 | - | No |
| gender | string | - | 255 | Yes |
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
| rateApplicableFor | string | - | 255 | Yes |
| smoking | string | - | 255 | Yes |

### BenefitInsuranceRateChartFixedAmount

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitInsuranceRateChart_effectiveStartDate | string | - | - | No |
| BenefitInsuranceRateChart_rateChartId | string | - | 128 | No |
| ageFrom | string | int64 | - | Yes |
| ageTo | string | int64 | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| employeeContribution | string | decimal | - | Yes |
| employerContribution | string | decimal | - | Yes |
| enrolleeOption | string | - | 128 | Yes |
| enrolleeOptionNav | Ref(BenefitInsuranceEnrolleeOptions) | - | - | No |
| externalCode | string | int64 | - | No |
| gender | string | - | 255 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| location | string | - | 32 | Yes |
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
| rateChartType | string | - | 255 | Yes |
| smoking | string | - | 255 | Yes |

### BenefitLeaveTravelReimbursementClaim

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEmployeeClaim_id | string | int64 | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| id | string | int64 | - | No |
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
| modeOfTravel | string | - | 128 | Yes |
| placeOfTravel | string | - | 255 | Yes |
| travelEndDate | string | - | - | Yes |
| travelStartDate | string | - | - | Yes |

### BenefitLegalEntity

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| company | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDate | string | - | - | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedDateWithTZ | string | - | - | Yes |
| legalEntityName | string | - | 128 | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemEffectiveStartDate | string | - | - | Yes |
| mdfSystemEntityId | string | - | 255 | Yes |
| mdfSystemObjectType | string | - | 255 | Yes |
| mdfSystemRecordId | string | - | 255 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| mdfSystemStatus | string | - | 255 | Yes |
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |

### BenefitLifeEventConfiguration

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| benefitEffectiveDateConfiguration | Object | - | - | Yes |
| benefitEnrollmentActionsConfiguration | Object | - | - | Yes |
| benefitEvent | string | - | 128 | Yes |
| benefitEventNav | Ref(BenefitEvent) | - | - | No |
| configurationId | string | - | 128 | No |
| configurationName | string | - | 128 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| exceptionForPeriod | string | - | 128 | Yes |
| exceptionWindowRule | string | - | 128 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| legalEntity | string | - | 128 | Yes |
| legalEntityNav | Ref(BenefitLegalEntity) | - | - | No |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### BenefitOpenEnrollmentCycleConfiguration

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| benefits | Object | - | - | Yes |
| cartWorkflowId | string | - | 32 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| instructionTextURL | string | - | 255 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| legalEntity | string | - | 128 | Yes |
| legalEntityNav | Ref(BenefitLegalEntity) | - | - | No |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| openEnrollmentId | string | - | 128 | No |
| openEnrollmentSchedule | string | - | 128 | Yes |
| openEnrollmentScheduleNav | Ref(BenefitSchedules) | - | - | No |
| tncURL | string | - | 255 | Yes |

### BenefitOverviewHyperlinkConfiguration

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| benefitOverviewHyperlinkDetails | Object | - | - | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| hyperlinkConfigurationId | string | - | 128 | No |
| hyperlinkConfigurationName | string | - | 128 | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### BenefitOverviewHyperlinkDetails

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitOverviewHyperlinkConfiguration_hyperlinkConfigurationId | string | - | 128 | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| eligibilityRule | string | - | 128 | Yes |
| id | string | - | 128 | No |
| label_de_DE | string | - | 70 | Yes |
| label_defaultValue | string | - | 70 | Yes |
| label_en_DEBUG | string | - | 70 | Yes |
| label_en_GB | string | - | 70 | Yes |
| label_en_US | string | - | 70 | Yes |
| label_es_ES | string | - | 70 | Yes |
| label_fr_FR | string | - | 70 | Yes |
| label_ja_JP | string | - | 70 | Yes |
| label_ko_KR | string | - | 70 | Yes |
| label_localized | string | - | 70 | Yes |
| label_nl_NL | string | - | 70 | Yes |
| label_pt_BR | string | - | 70 | Yes |
| label_pt_PT | string | - | 70 | Yes |
| label_ru_RU | string | - | 70 | Yes |
| label_zh_CN | string | - | 70 | Yes |
| label_zh_TW | string | - | 70 | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| url | string | - | 255 | Yes |

### BenefitPaymentOptions

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| Benefit_benefitId | string | - | 128 | No |
| Benefit_effectiveStartDate | string | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| isDefault | boolean | - | - | Yes |
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
| payComponent | string | - | 32 | No |
| paymentMode | string | - | 128 | Yes |

### BenefitPensionAdditionalContributionLimits

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| contributionLimitId | string | - | 128 | No |
| contributionLimitName | string | - | 128 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| currency | string | - | 128 | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| maxAdditionalContributionAmount | string | decimal | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| minAdditionalContributionAmount | string | decimal | - | Yes |

### BenefitPensionAdditionalEmployeeContributionDetail

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| Benefit_benefitId | string | - | 128 | No |
| Benefit_effectiveStartDate | string | - | - | No |
| avcContributionId | string | int64 | - | No |
| avcLimitApplicable | string | - | 128 | Yes |
| avcLimitConfiguration | string | - | 128 | Yes |
| avcLimitConfigurationNav | Object | - | - | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### BenefitPensionDependentNominees

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEnrollment_effectiveStartDate | string | - | - | No |
| BenefitEnrollment_id | string | int64 | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| dateOfBirth | string | - | - | Yes |
| dependentName | string | - | 128 | No |
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
| percentage | string | decimal | - | Yes |
| relationShipType | string | - | 128 | Yes |

### BenefitPensionEmployeeContributionDetail

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| Benefit_benefitId | string | - | 128 | No |
| Benefit_effectiveStartDate | string | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| employeeCanEditEmployeeContribution | boolean | - | - | Yes |
| employeeContributionAmount | string | decimal | - | Yes |
| employeeContributionPercentage | string | decimal | - | Yes |
| employeeContributionRule | string | - | 128 | Yes |
| employeeContributionType | string | - | 255 | Yes |
| employeeStatutoryMinimumAmount | string | decimal | - | Yes |
| employeeStatutoryMinimumPercentage | string | decimal | - | Yes |
| id | string | int64 | - | No |
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

### BenefitPensionEmployerContributionDetail

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| Benefit_benefitId | string | - | 128 | No |
| Benefit_effectiveStartDate | string | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| employeeCanEditEmployerContribution | boolean | - | - | Yes |
| employerContributionAmount | string | decimal | - | Yes |
| employerContributionPercentage | string | decimal | - | Yes |
| employerContributionRule | string | - | 128 | Yes |
| employerContributionType | string | - | 255 | Yes |
| employerStatutoryMinimumAmount | string | decimal | - | Yes |
| employerStatutoryMinimumPercentage | string | decimal | - | Yes |
| id | string | int64 | - | No |
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

### BenefitPensionEnrollmentContributionDetail

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEnrollment_effectiveStartDate | string | - | - | No |
| BenefitEnrollment_id | string | int64 | - | No |
| contributionId | string | int64 | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| employeeContributionAmt | string | decimal | - | Yes |
| employeeContributionPerc | string | decimal | - | Yes |
| employeeStatutoryMaxAmt | string | decimal | - | Yes |
| employeeStatutoryMaxPerc | string | decimal | - | Yes |
| employeeStatutoryMinAmt | string | decimal | - | Yes |
| employeeStatutoryMinPerc | string | decimal | - | Yes |
| employerContributionAmt | string | decimal | - | Yes |
| employerContributionPerc | string | decimal | - | Yes |
| employerStatutoryMaxAmt | string | decimal | - | Yes |
| employerStatutoryMaxPerc | string | decimal | - | Yes |
| employerStatutoryMinAmt | string | decimal | - | Yes |
| employerStatutoryMinPerc | string | decimal | - | Yes |
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

### BenefitPensionFund

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| Benefit_benefitId | string | - | 128 | No |
| Benefit_effectiveStartDate | string | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| employeeContributionPayComponent | string | - | 32 | Yes |
| employerContributionPayComponent | string | - | 32 | Yes |
| fundAgencyId | string | - | 255 | Yes |
| fundAgencyLink | string | - | 255 | Yes |
| fundName | string | - | 128 | Yes |
| fundNumber | string | - | 255 | Yes |
| id | string | int64 | - | No |
| isDefault | boolean | - | - | Yes |
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

### BenefitPensionFundEnrollmentContributionDetail

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEnrollment_effectiveStartDate | string | - | - | No |
| BenefitEnrollment_id | string | int64 | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| employeeContributionAmount | string | decimal | - | Yes |
| employeeContributionPercentage | string | decimal | - | Yes |
| employerContributionAmount | string | decimal | - | Yes |
| employerContributionPercentage | string | decimal | - | Yes |
| id | string | int64 | - | No |
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
| pensionFund | string | - | 128 | Yes |
| pensionFundNav | Ref(BenefitPensionFund) | - | - | No |

### BenefitPensionMinMaxContributionLimits

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| configurationTableId | string | - | 128 | No |
| configurationTableName | string | - | 128 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| currency | string | - | 128 | Yes |
| eeDefaultAmount | string | decimal | - | Yes |
| eeDefaultPercentage | string | decimal | - | Yes |
| eeMaxContributionAmount | string | decimal | - | Yes |
| eeMaxContributionPercentage | string | decimal | - | Yes |
| eeMinContributionAmount | string | decimal | - | Yes |
| eeMinContributionPercentage | string | decimal | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| employeeContributionType | string | - | 128 | Yes |
| employerContributionType | string | - | 128 | Yes |
| erDefaultAmount | string | decimal | - | Yes |
| erDefaultPercentage | string | decimal | - | Yes |
| erMaxContributionAmount | string | decimal | - | Yes |
| erMaxContributionPercentage | string | decimal | - | Yes |
| erMinContributionAmount | string | decimal | - | Yes |
| erMinContributionPercentage | string | decimal | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### BenefitPensionNonDependentNominees

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEnrollment_effectiveStartDate | string | - | - | No |
| BenefitEnrollment_id | string | int64 | - | No |
| address | string | - | 255 | Yes |
| contact | string | - | 255 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
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
| name | string | - | 255 | Yes |
| nomineesId | string | int64 | - | No |
| percentage | string | decimal | - | Yes |

### BenefitPensionStatutoryMinimumLookup

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| country | string | - | 128 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDate | string | - | - | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| employeeStatutoryMaximumAmount | string | decimal | - | Yes |
| employeeStatutoryMaximumPercentage | string | decimal | - | Yes |
| employeeStatutoryMinimumAmount | string | decimal | - | Yes |
| employeeStatutoryMinimumPercentage | string | decimal | - | Yes |
| employerStatutoryMaximumAmount | string | decimal | - | Yes |
| employerStatutoryMaximumPercentage | string | decimal | - | Yes |
| employerStatutoryMinimumAmount | string | decimal | - | Yes |
| employerStatutoryMinimumPercentage | string | decimal | - | Yes |
| externalCode | string | - | 128 | No |
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
| typeOfPension | string | - | 255 | Yes |

### BenefitProgram

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| amount | string | decimal | - | Yes |
| benefits | Object | - | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| currency | string | - | 128 | Yes |
| effectiveStartDate | string | - | - | No |
| eligibilityRuleId | string | - | 128 | Yes |
| exceptionWorkflowId | string | - | 32 | Yes |
| forms | Object | - | - | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemCreatedBy | string | - | 255 | Yes |
| mdfSystemCreatedDate | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
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
| multipleSelectionAllowed | boolean | - | - | Yes |
| policyDocuments | Object | - | - | Yes |
| programEnrollmentWorkflowId | string | - | 32 | Yes |
| programId | string | - | 128 | No |
| programName | string | - | 128 | Yes |
| programSchedule | string | - | 128 | Yes |
| programScheduleNav | Ref(BenefitSchedules) | - | - | No |
| status | string | - | 255 | Yes |
| supressClientDateValidation | boolean | - | - | Yes |
| usefulLinks | Object | - | - | Yes |

### BenefitProgramEnrollment

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| benefitProgram | string | - | 128 | Yes |
| benefitProgramDataSource | string | - | 255 | Yes |
| benefitProgramDataSourceWithExternalCode | string | - | 255 | Yes |
| benefitProgramEnrollmentDetail | Object | - | - | Yes |
| benefitProgramNav | Ref(BenefitProgram) | - | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| effectiveStatus | string | - | 255 | Yes |
| enrollmentDate | string | - | - | Yes |
| exception | string | - | 128 | Yes |
| exceptionNav | Ref(BenefitsException) | - | - | No |
| externalName | string | - | 128 | Yes |
| forms | Object | - | - | Yes |
| id | string | int64 | - | No |
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
| mdfSystemTransactionSequence | string | int64 | - | Yes |
| mdfSystemVersionId | string | int64 | - | Yes |
| policyDocuments | Object | - | - | Yes |
| programAmount | string | decimal | - | Yes |
| programCurrency | string | - | 128 | Yes |
| programEntitlementAmount | string | decimal | - | Yes |
| recordStatus | string | - | 255 | Yes |
| usefulLinks | Object | - | - | Yes |
| workerId | string | - | 100 | Yes |

### BenefitProgramEnrollmentDetail

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitProgramEnrollment_effectiveStartDate | string | - | - | No |
| BenefitProgramEnrollment_id | string | int64 | - | No |
| benefit | string | - | 128 | Yes |
| benefitAmount | string | decimal | - | Yes |
| benefitAmountForComparison | string | decimal | - | Yes |
| benefitCurrency | string | - | 128 | Yes |
| benefitNav | Object | - | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
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
| selectionId | string | int64 | - | No |

### BenefitProgramExceptionDetails

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitsException_exceptionId | string | int64 | - | No |
| adjustmentAmount | string | decimal | - | Yes |
| benefitProgram | string | - | 128 | No |
| benefitProgramNav | Object | - | - | Yes |
| benefitSchedulePeriodDataSourceWithExternalCode | string | - | 255 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
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
| schedulePeriod | string | - | 128 | Yes |
| schedulePeriodNav | Ref(BenefitSchedulePeriod) | - | - | No |

### BenefitSavingsPlanCatchUpDetail

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| ageAsOfDayInMonth | string | - | 128 | Yes |
| ageAsOfMonth | string | - | 128 | Yes |
| ageAsOfYear | string | - | 128 | Yes |
| catchUpAge | string | int64 | - | Yes |
| catchUpAmount | string | decimal | - | Yes |
| catchUpDetailCode | string | - | 128 | No |
| catchUpDetailName_de_DE | string | - | 255 | Yes |
| catchUpDetailName_defaultValue | string | - | 255 | Yes |
| catchUpDetailName_en_DEBUG | string | - | 255 | Yes |
| catchUpDetailName_en_GB | string | - | 255 | Yes |
| catchUpDetailName_en_US | string | - | 255 | Yes |
| catchUpDetailName_es_ES | string | - | 255 | Yes |
| catchUpDetailName_fr_FR | string | - | 255 | Yes |
| catchUpDetailName_ja_JP | string | - | 255 | Yes |
| catchUpDetailName_ko_KR | string | - | 255 | Yes |
| catchUpDetailName_localized | string | - | 255 | Yes |
| catchUpDetailName_nl_NL | string | - | 255 | Yes |
| catchUpDetailName_pt_BR | string | - | 255 | Yes |
| catchUpDetailName_pt_PT | string | - | 255 | Yes |
| catchUpDetailName_ru_RU | string | - | 255 | Yes |
| catchUpDetailName_zh_CN | string | - | 255 | Yes |
| catchUpDetailName_zh_TW | string | - | 255 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### BenefitSavingsPlanContingentBeneficiary

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEnrollment_effectiveStartDate | string | - | - | No |
| BenefitEnrollment_id | string | - | 38 | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| dependentDateOfBirth | string | - | - | Yes |
| dependentId | string | int64 | - | Yes |
| dependentName | string | - | 255 | Yes |
| dependentRelationShipType | string | - | 128 | Yes |
| externalCode | string | int64 | - | No |
| isDependent | boolean | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| nonDependentDateOfBirth | string | - | - | Yes |
| nonDependentName | string | - | 255 | Yes |
| nonDependentRelationShipType | string | - | 255 | Yes |
| percentage | string | decimal | - | Yes |
| referenceId | string | - | 255 | Yes |

### BenefitSavingsPlanERContributionConfig

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| employerContributionDetail | Object | - | - | Yes |
| employerContributionId | string | - | 128 | No |
| employerContributionName_de_DE | string | - | 255 | Yes |
| employerContributionName_defaultValue | string | - | 255 | Yes |
| employerContributionName_en_DEBUG | string | - | 255 | Yes |
| employerContributionName_en_GB | string | - | 255 | Yes |
| employerContributionName_en_US | string | - | 255 | Yes |
| employerContributionName_es_ES | string | - | 255 | Yes |
| employerContributionName_fr_FR | string | - | 255 | Yes |
| employerContributionName_ja_JP | string | - | 255 | Yes |
| employerContributionName_ko_KR | string | - | 255 | Yes |
| employerContributionName_localized | string | - | 255 | Yes |
| employerContributionName_nl_NL | string | - | 255 | Yes |
| employerContributionName_pt_BR | string | - | 255 | Yes |
| employerContributionName_pt_PT | string | - | 255 | Yes |
| employerContributionName_ru_RU | string | - | 255 | Yes |
| employerContributionName_zh_CN | string | - | 255 | Yes |
| employerContributionName_zh_TW | string | - | 255 | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### BenefitSavingsPlanERContributionConfigDetail

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitSavingsPlanERContributionConfig_effectiveStartDate | string | - | - | No |
| BenefitSavingsPlanERContributionConfig_employerContributionId | string | - | 128 | No |
| contributionAmount | string | decimal | - | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| day | string | - | 128 | Yes |
| employerContributionDetailId | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| month | string | - | 128 | Yes |

### BenefitSavingsPlanEnrollmentContributionDetail

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEnrollment_effectiveStartDate | string | - | - | No |
| BenefitEnrollment_id | string | int64 | - | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| id | string | int64 | - | No |
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
| pensionFund | string | - | 128 | Yes |
| pensionFundNav | Ref(BenefitPensionFund) | - | - | No |
| postTaxAmountBonus | string | decimal | - | Yes |
| postTaxAmountRegular | string | decimal | - | Yes |
| postTaxPercentageBonus | string | decimal | - | Yes |
| postTaxPercentageRegular | string | decimal | - | Yes |
| preTaxAmountBonus | string | decimal | - | Yes |
| preTaxAmountRegular | string | decimal | - | Yes |
| preTaxPercentageBonus | string | decimal | - | Yes |
| preTaxPercentageRegular | string | decimal | - | Yes |
| rollOverBonus | boolean | - | - | Yes |
| rollOverRegular | boolean | - | - | Yes |
| startPostTaxBonus | boolean | - | - | Yes |
| startPostTaxRegular | boolean | - | - | Yes |

### BenefitSavingsPlanEnrollmentDetails

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEnrollment_effectiveStartDate | string | - | - | No |
| BenefitEnrollment_id | string | - | 38 | No |
| annualContributionAmount | string | decimal | - | Yes |
| annualMaxContributionAmount | string | decimal | - | Yes |
| annualMinContributionAmount | string | decimal | - | Yes |
| calculatedAnnualLimit | string | decimal | - | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| empBenefitPayComponent | string | - | 32 | Yes |
| estimatedAnnualContribution | string | decimal | - | Yes |
| estimatedEmployerContributionAmount | string | decimal | - | Yes |
| hsaAnnualContributionAmount | string | decimal | - | Yes |
| hsaPayPeriodContributionAmount | string | decimal | - | Yes |
| id | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| payPeriodContributionAmount | string | decimal | - | Yes |
| payPeriodsRemaining | string | int64 | - | Yes |
| savingsPlanSubType | string | - | 128 | Yes |
| ytdContribution | string | decimal | - | Yes |

### BenefitSavingsPlanPrimaryBeneficiary

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| BenefitEnrollment_effectiveStartDate | string | - | - | No |
| BenefitEnrollment_id | string | - | 38 | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| dependentDateOfBirth | string | - | - | Yes |
| dependentId | string | int64 | - | Yes |
| dependentName | string | - | 255 | Yes |
| dependentRelationShipType | string | - | 128 | Yes |
| externalCode | string | int64 | - | No |
| isDependent | boolean | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| nonDependentDateOfBirth | string | - | - | Yes |
| nonDependentName | string | - | 255 | Yes |
| nonDependentRelationShipType | string | - | 255 | Yes |
| percentage | string | decimal | - | Yes |
| referenceId | string | - | 255 | Yes |

### BenefitSavingsPlanSubType

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| benefitSavingsPlanID | string | - | 128 | No |
| country | string | - | 128 | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| externalName | string | - | 128 | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemCreatedBy | string | - | 255 | Yes |
| mdfSystemCreatedDate | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
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

### BenefitSavingsPlanSubTypeCountryLookup

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| benefitSavingsPlanSubTypes | Object | - | - | Yes |
| country | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemCreatedBy | string | - | 255 | Yes |
| mdfSystemCreatedDate | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
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

### BenefitSavingsPlanTierConfiguration

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| Benefit_benefitId | string | - | 128 | No |
| Benefit_effectiveStartDate | string | - | - | No |
| annualMaxContributionAmount | string | decimal | - | Yes |
| annualMinContributionAmount | string | decimal | - | Yes |
| coverageTier | string | - | 128 | No |
| coverageTierNav | Ref(BenefitInsuranceEnrolleeOptions) | - | - | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| employerContributionConfig | string | - | 128 | Yes |
| employerContributionConfigNav | Object | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### BenefitSchedulePeriod

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| balanceCarryForwardUptoDate | string | - | - | Yes |
| claimWindowEndDate | string | - | - | Yes |
| claimWindowStartDate | string | - | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| enrollmentEffectiveFrom | string | - | - | Yes |
| enrollmentValidityEndDate | string | - | - | Yes |
| enrollmentWindowEndDate | string | - | - | Yes |
| enrollmentWindowStartDate | string | - | - | Yes |
| id | string | - | 128 | No |
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
| periodName | string | - | 128 | Yes |

### BenefitSchedules

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| id | string | - | 128 | No |
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
| periods | Object | - | - | Yes |
| scheduleName | string | - | 128 | Yes |

### BenefitsConfigUIScreenLookup

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
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
| objectType | string | - | 128 | No |
| screenId | string | - | 255 | Yes |

### BenefitsConfirmationStatementConfiguration

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| legalEntity | string | - | 128 | No |
| legalEntityNav | Ref(BenefitLegalEntity) | - | - | No |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| templateId | string | - | 128 | Yes |

### BenefitsException

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| benefitExceptionDetails | Object | - | - | Yes |
| benefitProgramExceptionDetails | Object | - | - | Yes |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| eventDate | string | - | - | Yes |
| exceptionCreationDate | string | - | - | Yes |
| exceptionEndDate | string | - | - | Yes |
| exceptionFor | string | - | 255 | Yes |
| exceptionId | string | int64 | - | No |
| exceptionName | string | - | 128 | Yes |
| exceptionStartDate | string | - | - | Yes |
| isAutomation | boolean | - | - | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| legalEntity | string | - | 128 | Yes |
| lifeEventConfiguration | string | - | 128 | Yes |
| lifeEventConfigurationNav | Object | - | - | Yes |
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
| newExceptionWindow | boolean | - | - | Yes |
| workerId | string | - | 100 | Yes |

### BenefitsIntegrationOneTimeInfo

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| benefit | string | - | 128 | Yes |
| benefitEnrollment | string | - | 128 | Yes |
| benefitEnrollmentNav | Object | - | - | Yes |
| benefitNav | Object | - | - | Yes |
| benefitSchedulePeriod | string | - | 128 | Yes |
| benefitSchedulePeriodNav | Ref(BenefitSchedulePeriod) | - | - | No |
| category | string | - | 128 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| currency | string | - | 128 | Yes |
| effectiveStatus | string | - | 128 | Yes |
| externalName | string | - | 128 | Yes |
| id | string | - | 128 | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| payCompDate | string | - | - | Yes |
| payComponent | string | - | 32 | Yes |
| recordStatus | string | - | 128 | Yes |
| reference | string | - | 128 | Yes |
| unitOfMeasure | string | - | 128 | Yes |
| value | string | decimal | - | Yes |
| workerId | string | - | 100 | Yes |

### BenefitsIntegrationRecurringInfo

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| benefit | string | - | 128 | Yes |
| benefitEnrollment | string | - | 128 | Yes |
| benefitEnrollmentNav | Ref(BenefitEnrollment) | - | - | No |
| benefitNav | Ref(Benefit) | - | - | No |
| benefitSchedulePeriod | string | - | 128 | Yes |
| benefitSchedulePeriodNav | Ref(BenefitSchedulePeriod) | - | - | No |
| category | string | - | 128 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| currency | string | - | 128 | Yes |
| effectiveStatus | string | - | 128 | Yes |
| externalName | string | - | 128 | Yes |
| frequency | string | - | 32 | Yes |
| id | string | - | 128 | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| payCompBeginDate | string | - | - | No |
| payCompEndDate | string | - | - | Yes |
| payComponent | string | - | 32 | Yes |
| recordStatus | string | - | 128 | Yes |
| reference | string | - | 128 | Yes |
| unitOfMeasure | string | - | 128 | Yes |
| value | string | decimal | - | Yes |
| workerId | string | - | 100 | Yes |

### EmployeeWithEmployerMatchContributionEntries

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| EmployeeWithEmployerMatchContributions_configurationId | string | - | 128 | No |
| EmployeeWithEmployerMatchContributions_effectiveStartDate | string | - | - | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| eeContributionPercentage | string | decimal | - | Yes |
| eeERMatchValuesID | string | - | 128 | No |
| erContributionPercentage | string | decimal | - | Yes |
| isDefault | boolean | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### EmployeeWithEmployerMatchContributions

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| configurationId | string | - | 128 | No |
| configurationName | string | - | 128 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| effectiveEndDate | string | - | - | Yes |
| effectiveStartDate | string | - | - | No |
| employeeWithEmployerMatchContributionEntries | Object | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### IRSPremiumTable

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| ageVerificationDate | string | - | 128 | Yes |
| ageVerificationMonth | string | - | 128 | Yes |
| ageVerificationYear | string | - | 128 | Yes |
| configurationId | string | - | 128 | No |
| configurationName | string | - | 128 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| currency | string | - | 128 | Yes |
| effectiveStartDate | string | - | - | No |
| exemptedAmount | string | decimal | - | Yes |
| factor | string | decimal | - | Yes |
| imputedCostForAgeRanges | Object | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### ImputedCostForAgeRanges

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| IRSPremiumTable_configurationId | string | - | 128 | No |
| IRSPremiumTable_effectiveStartDate | string | - | - | No |
| ageFrom | string | int64 | - | Yes |
| ageTo | string | int64 | - | Yes |
| costPerAge | string | decimal | - | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | - | 128 | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### InsuranceBenefitDetails

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| Benefit_benefitId | string | - | 128 | No |
| Benefit_effectiveStartDate | string | - | - | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| decimalPrecisionSettingForContributionAmount | boolean | - | - | Yes |
| defaultCoverage | string | - | 128 | Yes |
| defaultCoverageNav | Ref(BenefitInsuranceCoverage) | - | - | No |
| defaultEnrolleeOptions | string | - | 128 | Yes |
| defaultEnrolleeOptionsNav | Ref(BenefitInsuranceEnrolleeOptions) | - | - | No |
| defaultPlan | string | - | 128 | Yes |
| defaultPlanNav | Object | - | - | Yes |
| enrollmentApplicableFor | string | - | 128 | Yes |
| externalCode | string | int64 | - | No |
| insuranceFieldConfiguration | Object | - | - | Yes |
| insurancePlans | Object | - | - | Yes |
| insuranceType | string | - | 128 | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| nomineeRelevant | boolean | - | - | Yes |

### InsuranceEnrollmentFieldsConfiguration

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| Benefit_benefitId | string | - | 128 | No |
| Benefit_effectiveStartDate | string | - | - | No |
| InsuranceBenefitDetails_externalCode | string | int64 | - | No |
| configurationId | string | int64 | - | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| insuranceFieldName | string | - | 128 | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| visibilitySettingsEnum | string | - | 128 | Yes |

### LifeEventForBenefit

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| daysLimitToReport | string | int64 | - | Yes |
| dependentInfoLinkApplicable | boolean | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| legalEntities | Object | - | - | Yes |
| lifeEventId | string | - | 128 | No |
| lifeEventName | string | - | 128 | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| personalInfoLinkApplicable | boolean | - | - | Yes |

### PensionBandingConfiguration

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| bandingConfigurationId | string | - | 128 | No |
| bandingConfigurationName | string | - | 128 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| currency | string | - | 128 | Yes |
| effectiveStartDate | string | - | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemEffectiveEndDate | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| pensionBandingConfigurationDetails | Object | - | - | Yes |

### PensionBandingConfigurationDetails

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| PensionBandingConfiguration_bandingConfigurationId | string | - | 128 | No |
| PensionBandingConfiguration_effectiveStartDate | string | - | - | No |
| bandingDetailsId | string | - | 128 | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| employeePercentage | string | decimal | - | Yes |
| employerPercentage | string | decimal | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lowerBandValue | string | decimal | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| upperBandValue | string | decimal | - | Yes |

### SavingsAccountBenefitDetails

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| Benefit_benefitId | string | - | 128 | No |
| Benefit_effectiveStartDate | string | - | - | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| externalCode | string | int64 | - | No |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| savingsAccountCountry | string | - | 128 | Yes |
| savingsAccountUSA | Ref(SavingsAccountUSA) | - | - | No |

### SavingsAccountDeductionDetails

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| Benefit_benefitId | string | - | 128 | No |
| Benefit_effectiveStartDate | string | - | - | No |
| SavingsAccountBenefitDetails_externalCode | string | int64 | - | No |
| SavingsAccountUSA_externalCode | string | int64 | - | No |
| basePayComponent | string | - | 32 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| deductionDetailId | string | int64 | - | No |
| deductionPayComponent | string | - | 32 | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| payFrequency | string | - | 32 | Yes |

### SavingsAccountTierConfiguration

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| Benefit_benefitId | string | - | 128 | No |
| Benefit_effectiveStartDate | string | - | - | No |
| SavingsAccountBenefitDetails_externalCode | string | int64 | - | No |
| SavingsAccountUSA_externalCode | string | int64 | - | No |
| annualMaxContributionAmount | string | decimal | - | Yes |
| annualMinContributionAmount | string | decimal | - | Yes |
| coverageTier | string | - | 128 | No |
| coverageTierNav | Ref(BenefitInsuranceEnrolleeOptions) | - | - | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |

### SavingsAccountUSA

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| Benefit_benefitId | string | - | 128 | No |
| Benefit_effectiveStartDate | string | - | - | No |
| SavingsAccountBenefitDetails_externalCode | string | int64 | - | No |
| annualMaxContributionAmount | string | decimal | - | Yes |
| annualMaxPayComponent | string | - | 32 | Yes |
| annualMinContributionAmount | string | decimal | - | Yes |
| beneficiaryRelevant | boolean | - | - | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| empAnnualLimitPayComponent | string | - | 32 | Yes |
| employerContributionHSA | string | - | 128 | Yes |
| employerContributionHSANav | Object | - | - | Yes |
| employerPayComponent | string | - | 32 | Yes |
| externalCode | string | int64 | - | No |
| isEmployerContributionRequired | boolean | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| savingsAccountDeductionDetails | Object | - | - | Yes |
| savingsAccountTierConfiguration | Object | - | - | Yes |
| savingsPlanCatchUpDetail | string | - | 128 | Yes |
| savingsPlanCatchUpDetailNav | Object | - | - | Yes |
| savingsPlanSubType | string | - | 128 | Yes |

## Object Primary Keys

Primary key fields identified for each entity:

- **ACAReportingDependentDetails**: externalCode, ACAReportingInformation_externalCode, dependentId
- **ACAReportingInformation**: workerId, externalCode, exceptionId
- **Benefit**: mdfSystemRecordId, employeeEnrollmentWorkflowId, employeeClaimWorkflowId, mdfSystemEntityId, exceptionWorkflowId, mdfSystemVersionId, effectiveStartDate, eligibilityRuleId, benefitId
- **BenefitAutomaticActionConfiguration**: id, BenefitLifeEventConfiguration_effectiveStartDate, BenefitLifeEventConfiguration_configurationId
- **BenefitBalanceCarryForward**: mdfSystemRecordId, Benefit_effectiveStartDate, Benefit_benefitId, externalCode, mdfSystemEntityId, mdfSystemVersionId
- **BenefitBalanceCarryForwardDetail**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, BenefitClaimAccumulation_externalCode
- **BenefitClaimAccumulation**: workerId, externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId
- **BenefitCompanyCar**: Benefit_effectiveStartDate, Benefit_benefitId, mdfSystemRecordId, mdfSystemEntityId, benefitCompanyCarId, mdfSystemVersionId
- **BenefitCompanyCarAllowedModels**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **BenefitCompanyCarClaim**: mdfSystemRecordId, id, mdfSystemEntityId, BenefitEmployeeClaim_id, mdfSystemVersionId
- **BenefitCompanyCarEnrollment**: mdfSystemRecordId, id, mdfSystemEntityId, mdfSystemVersionId, BenefitEnrollment_id, BenefitEnrollment_effectiveStartDate
- **BenefitCompanyCarLeaseServiceProvider**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **BenefitCompanyCarRecommendedVendors**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **BenefitCompanyHousing**: Benefit_effectiveStartDate, Benefit_benefitId, mdfSystemRecordId, id, mdfSystemEntityId, mdfSystemVersionId
- **BenefitCompanyHousingEnrollment**: mdfSystemRecordId, id, mdfSystemEntityId, mdfSystemVersionId, BenefitEnrollment_id, personalId, BenefitEnrollment_effectiveStartDate
- **BenefitContact**: mdfSystemRecordId, externalCode, mdfSystemEntityId, mdfSystemVersionId, contactEmployeeId
- **BenefitDeductibleAllowanceEnrollment**: id, BenefitEnrollment_id, BenefitEnrollment_effectiveStartDate
- **BenefitDeductionDetails**: dedcutionDetailId, Benefit_effectiveStartDate, Benefit_benefitId
- **BenefitDependentDetail**: BenefitEmployeeClaim_id, mdfSystemEntityId, mdfSystemRecordId, mdfSystemVersionId
- **BenefitDocuments**: mdfSystemRecordId, id, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **BenefitEffectiveDateConfiguration**: BenefitLifeEventConfiguration_effectiveStartDate, BenefitLifeEventConfiguration_configurationId
- **BenefitEmployeeClaim**: workerId, mdfSystemRecordId, id, mdfSystemEntityId, mdfSystemVersionId, nrpId
- **BenefitEmployeeClaimDetail**: mdfSystemRecordId, id, mdfSystemEntityId, BenefitEmployeeClaim_id, mdfSystemVersionId
- **BenefitEmployeeLifeEventDeclarationForm**: workerId, lifeEventId, employeeLifeEventId
- **BenefitEmployeeOptoutRequests**: workerId
- **BenefitEnrollment**: workerId, mdfSystemRecordId, id, mdfSystemEntityId, compensationId, mdfSystemVersionId, effectiveStartDate, previousEnrollmentId
- **BenefitEnrollmentDependencyConfiguration**: effectiveStartDate, benefitDependencyId
- **BenefitEnrollmentDependencyDetails**: BenefitEnrollmentDependencyConfiguration_benefitDependencyId, externalCode, BenefitEnrollmentDependencyConfiguration_effectiveStartDate
- **BenefitEnrollmentGroup**: effectiveStartDate, id, workerId
- **BenefitEnrollmentOptoutDetails**: id, BenefitEmployeeOptoutRequests_workerId, benefitEnrollmentId
- **BenefitEvent**: effectiveStartDate, eventId
- **BenefitExceptionDetails**: mdfSystemEntityId, mdfSystemRecordId, mdfSystemVersionId, BenefitsException_exceptionId
- **BenefitFuelReimbursement**: externalCode, mdfSystemRecordId, mdfSystemEntityId, BenefitEmployeeClaim_id, mdfSystemVersionId
- **BenefitFuelReimbursementClaimDetail**: mdfSystemRecordId, id, mdfSystemEntityId, BenefitEmployeeClaim_id, mdfSystemVersionId
- **BenefitHSAEmployerContribution**: effectiveStartDate, externalCode
- **BenefitHSAEmployerContributionDetail**: externalCode, BenefitHSAEmployerContribution_effectiveStartDate, BenefitHSAEmployerContribution_externalCode
- **BenefitHSAEmployerContributionTierDetail**: BenefitHSAEmployerContributionDetail_externalCode, BenefitHSAEmployerContribution_effectiveStartDate, BenefitHSAEmployerContribution_externalCode
- **BenefitHyperlinkConfiguration**: id, Benefit_effectiveStartDate, Benefit_benefitId
- **BenefitInsuranceCoverage**: coverageId, mdfSystemEntityId, mdfSystemRecordId, mdfSystemVersionId
- **BenefitInsuranceCoverageDetails**: externalCode, mdfSystemRecordId, BenefitInsuranceCoverageOptions_externalCode, mdfSystemEntityId, BenefitInsurancePlan_effectiveStartDate, mdfSystemVersionId, BenefitInsurancePlan_id
- **BenefitInsuranceCoverageOptions**: externalCode, mdfSystemRecordId, mdfSystemEntityId, BenefitInsurancePlan_effectiveStartDate, mdfSystemVersionId, BenefitInsurancePlan_id
- **BenefitInsuranceDependentDetail**: mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, BenefitEnrollment_id, BenefitInsurancePlanEnrollmentDetails_externalCode, BenefitEnrollment_effectiveStartDate
- **BenefitInsuranceEnrolleeOptions**: id, mdfSystemRecordId, mdfSystemVersionId, mdfSystemEntityId
- **BenefitInsuranceEnrolleeType**: mdfSystemEntityId, mdfSystemRecordId, mdfSystemVersionId
- **BenefitInsurancePlan**: mdfSystemRecordId, id, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **BenefitInsurancePlanEnrollmentDetails**: mdfSystemRecordId, externalCode, mdfSystemEntityId, mdfSystemVersionId, BenefitEnrollment_id, BenefitEnrollment_effectiveStartDate
- **BenefitInsurancePlanUSA**: BenefitInsurancePlan_id, id, BenefitInsurancePlan_effectiveStartDate
- **BenefitInsuranceProvider**: mdfSystemEntityId, mdfSystemRecordId, mdfSystemVersionId, providerId
- **BenefitInsuranceRateChart**: mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate, rateChartId
- **BenefitInsuranceRateChartEnrollee**: mdfSystemRecordId, externalCode, mdfSystemEntityId, BenefitInsuranceRateChart_rateChartId, mdfSystemVersionId, BenefitInsuranceRateChart_effectiveStartDate
- **BenefitInsuranceRateChartFixedAmount**: mdfSystemRecordId, externalCode, mdfSystemEntityId, BenefitInsuranceRateChart_rateChartId, mdfSystemVersionId, BenefitInsuranceRateChart_effectiveStartDate
- **BenefitLeaveTravelReimbursementClaim**: mdfSystemRecordId, id, mdfSystemEntityId, BenefitEmployeeClaim_id, mdfSystemVersionId
- **BenefitLegalEntity**: mdfSystemEntityId, mdfSystemRecordId, mdfSystemVersionId
- **BenefitLifeEventConfiguration**: configurationId, effectiveStartDate
- **BenefitOpenEnrollmentCycleConfiguration**: effectiveStartDate, openEnrollmentId, cartWorkflowId
- **BenefitOverviewHyperlinkConfiguration**: hyperlinkConfigurationId
- **BenefitOverviewHyperlinkDetails**: id, BenefitOverviewHyperlinkConfiguration_hyperlinkConfigurationId
- **BenefitPaymentOptions**: Benefit_effectiveStartDate, Benefit_benefitId, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId
- **BenefitPensionAdditionalContributionLimits**: effectiveStartDate, contributionLimitId
- **BenefitPensionAdditionalEmployeeContributionDetail**: avcContributionId, Benefit_effectiveStartDate, Benefit_benefitId
- **BenefitPensionDependentNominees**: mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, BenefitEnrollment_id, BenefitEnrollment_effectiveStartDate
- **BenefitPensionEmployeeContributionDetail**: Benefit_effectiveStartDate, Benefit_benefitId, mdfSystemRecordId, id, mdfSystemEntityId, mdfSystemVersionId
- **BenefitPensionEmployerContributionDetail**: Benefit_effectiveStartDate, Benefit_benefitId, mdfSystemRecordId, id, mdfSystemEntityId, mdfSystemVersionId
- **BenefitPensionEnrollmentContributionDetail**: mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, BenefitEnrollment_id, contributionId, BenefitEnrollment_effectiveStartDate
- **BenefitPensionFund**: Benefit_effectiveStartDate, Benefit_benefitId, mdfSystemRecordId, id, mdfSystemEntityId, mdfSystemVersionId, fundAgencyId
- **BenefitPensionFundEnrollmentContributionDetail**: mdfSystemRecordId, id, mdfSystemEntityId, mdfSystemVersionId, BenefitEnrollment_id, BenefitEnrollment_effectiveStartDate
- **BenefitPensionMinMaxContributionLimits**: effectiveStartDate, configurationTableId
- **BenefitPensionNonDependentNominees**: mdfSystemRecordId, mdfSystemEntityId, nomineesId, mdfSystemVersionId, BenefitEnrollment_id, BenefitEnrollment_effectiveStartDate
- **BenefitPensionStatutoryMinimumLookup**: externalCode, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **BenefitProgram**: mdfSystemRecordId, programId, mdfSystemEntityId, exceptionWorkflowId, mdfSystemVersionId, effectiveStartDate, eligibilityRuleId, programEnrollmentWorkflowId
- **BenefitProgramEnrollment**: workerId, mdfSystemRecordId, id, mdfSystemEntityId, mdfSystemVersionId, effectiveStartDate
- **BenefitProgramEnrollmentDetail**: selectionId, mdfSystemRecordId, BenefitProgramEnrollment_id, mdfSystemEntityId, mdfSystemVersionId, BenefitProgramEnrollment_effectiveStartDate
- **BenefitProgramExceptionDetails**: mdfSystemEntityId, mdfSystemRecordId, mdfSystemVersionId, BenefitsException_exceptionId
- **BenefitSavingsPlanCatchUpDetail**: effectiveStartDate
- **BenefitSavingsPlanContingentBeneficiary**: externalCode, dependentId, referenceId, BenefitEnrollment_id, BenefitEnrollment_effectiveStartDate
- **BenefitSavingsPlanERContributionConfig**: effectiveStartDate, employerContributionId
- **BenefitSavingsPlanERContributionConfigDetail**: BenefitSavingsPlanERContributionConfig_employerContributionId, employerContributionDetailId, BenefitSavingsPlanERContributionConfig_effectiveStartDate
- **BenefitSavingsPlanEnrollmentContributionDetail**: mdfSystemRecordId, id, mdfSystemEntityId, mdfSystemVersionId, BenefitEnrollment_id, BenefitEnrollment_effectiveStartDate
- **BenefitSavingsPlanEnrollmentDetails**: id, BenefitEnrollment_id, BenefitEnrollment_effectiveStartDate
- **BenefitSavingsPlanPrimaryBeneficiary**: externalCode, dependentId, referenceId, BenefitEnrollment_id, BenefitEnrollment_effectiveStartDate
- **BenefitSavingsPlanSubType**: effectiveStartDate, mdfSystemEntityId, mdfSystemRecordId, mdfSystemVersionId
- **BenefitSavingsPlanSubTypeCountryLookup**: effectiveStartDate, mdfSystemEntityId, mdfSystemRecordId, mdfSystemVersionId
- **BenefitSavingsPlanTierConfiguration**: Benefit_effectiveStartDate, Benefit_benefitId
- **BenefitSchedulePeriod**: id, mdfSystemRecordId, mdfSystemVersionId, mdfSystemEntityId
- **BenefitSchedules**: id, mdfSystemRecordId, mdfSystemVersionId, mdfSystemEntityId
- **BenefitsConfigUIScreenLookup**: screenId, mdfSystemEntityId, mdfSystemRecordId, mdfSystemVersionId
- **BenefitsConfirmationStatementConfiguration**: effectiveStartDate, templateId
- **BenefitsException**: workerId, mdfSystemRecordId, mdfSystemEntityId, mdfSystemVersionId, exceptionId
- **BenefitsIntegrationOneTimeInfo**: workerId, id
- **BenefitsIntegrationRecurringInfo**: workerId, id
- **EmployeeWithEmployerMatchContributionEntries**: EmployeeWithEmployerMatchContributions_effectiveStartDate, EmployeeWithEmployerMatchContributions_configurationId
- **EmployeeWithEmployerMatchContributions**: configurationId, effectiveStartDate
- **IRSPremiumTable**: configurationId, effectiveStartDate
- **ImputedCostForAgeRanges**: IRSPremiumTable_configurationId, externalCode, IRSPremiumTable_effectiveStartDate
- **InsuranceBenefitDetails**: Benefit_effectiveStartDate, Benefit_benefitId, externalCode
- **InsuranceEnrollmentFieldsConfiguration**: configurationId, InsuranceBenefitDetails_externalCode, Benefit_effectiveStartDate, Benefit_benefitId
- **LifeEventForBenefit**: lifeEventId
- **PensionBandingConfiguration**: bandingConfigurationId, effectiveStartDate
- **PensionBandingConfigurationDetails**: PensionBandingConfiguration_bandingConfigurationId, bandingDetailsId, PensionBandingConfiguration_effectiveStartDate
- **SavingsAccountBenefitDetails**: Benefit_effectiveStartDate, Benefit_benefitId, externalCode
- **SavingsAccountDeductionDetails**: Benefit_effectiveStartDate, Benefit_benefitId, SavingsAccountUSA_externalCode, SavingsAccountBenefitDetails_externalCode, deductionDetailId
- **SavingsAccountTierConfiguration**: Benefit_effectiveStartDate, Benefit_benefitId, SavingsAccountBenefitDetails_externalCode, SavingsAccountUSA_externalCode
- **SavingsAccountUSA**: Benefit_effectiveStartDate, Benefit_benefitId, SavingsAccountBenefitDetails_externalCode, externalCode

## Object's Ingestion Type

Ingestion types are determined based on API capabilities:


| Entity | Ingestion Type | Cursor Field |
|--------|---------------|--------------|
| ACAReportingDependentDetails | cdc | lastModifiedDateTime |
| ACAReportingInformation | cdc | lastModifiedDateTime |
| Benefit | cdc | lastModifiedDateTime |
| BenefitAutomaticActionConfiguration | cdc | lastModifiedDateTime |
| BenefitBalanceCarryForward | cdc | lastModifiedDateTime |
| BenefitBalanceCarryForwardDetail | cdc | lastModifiedDateTime |
| BenefitClaimAccumulation | cdc | lastModifiedDateTime |
| BenefitCompanyCar | cdc | lastModifiedDateTime |
| BenefitCompanyCarAllowedModels | cdc | lastModifiedDateTime |
| BenefitCompanyCarClaim | cdc | lastModifiedDateTime |
| BenefitCompanyCarEnrollment | cdc | lastModifiedDateTime |
| BenefitCompanyCarLeaseServiceProvider | cdc | lastModifiedDateTime |
| BenefitCompanyCarRecommendedVendors | cdc | lastModifiedDateTime |
| BenefitCompanyHousing | cdc | lastModifiedDateTime |
| BenefitCompanyHousingEnrollment | cdc | lastModifiedDateTime |
| BenefitContact | cdc | lastModifiedDateTime |
| BenefitDeductibleAllowanceEnrollment | cdc | lastModifiedDateTime |
| BenefitDeductionDetails | cdc | lastModifiedDateTime |
| BenefitDependentDetail | cdc | lastModifiedDateTime |
| BenefitDocuments | cdc | lastModifiedDateTime |
| BenefitEffectiveDateConfiguration | cdc | lastModifiedDateTime |
| BenefitEmployeeClaim | cdc | lastModifiedDateTime |
| BenefitEmployeeClaimDetail | cdc | lastModifiedDateTime |
| BenefitEmployeeLifeEventDeclarationForm | cdc | lastModifiedDateTime |
| BenefitEmployeeOptoutRequests | cdc | lastModifiedDateTime |
| BenefitEnrollment | cdc | lastModifiedDateTime |
| BenefitEnrollmentDependencyConfiguration | cdc | lastModifiedDateTime |
| BenefitEnrollmentDependencyDetails | cdc | lastModifiedDateTime |
| BenefitEnrollmentGroup | cdc | lastModifiedDateTime |
| BenefitEnrollmentOptoutDetails | cdc | lastModifiedDateTime |
| BenefitEvent | cdc | lastModifiedDateTime |
| BenefitExceptionDetails | cdc | lastModifiedDateTime |
| BenefitFuelReimbursement | cdc | lastModifiedDateTime |
| BenefitFuelReimbursementClaimDetail | cdc | lastModifiedDateTime |
| BenefitHSAEmployerContribution | cdc | lastModifiedDateTime |
| BenefitHSAEmployerContributionDetail | cdc | lastModifiedDateTime |
| BenefitHSAEmployerContributionTierDetail | cdc | lastModifiedDateTime |
| BenefitHyperlinkConfiguration | cdc | lastModifiedDateTime |
| BenefitInsuranceCoverage | cdc | lastModifiedDateTime |
| BenefitInsuranceCoverageDetails | cdc | lastModifiedDateTime |
| BenefitInsuranceCoverageOptions | cdc | lastModifiedDateTime |
| BenefitInsuranceDependentDetail | cdc | lastModifiedDateTime |
| BenefitInsuranceEnrolleeOptions | cdc | lastModifiedDateTime |
| BenefitInsuranceEnrolleeType | cdc | lastModifiedDateTime |
| BenefitInsurancePlan | cdc | lastModifiedDateTime |
| BenefitInsurancePlanEnrollmentDetails | cdc | lastModifiedDateTime |
| BenefitInsurancePlanUSA | cdc | lastModifiedDateTime |
| BenefitInsuranceProvider | cdc | lastModifiedDateTime |
| BenefitInsuranceRateChart | cdc | lastModifiedDateTime |
| BenefitInsuranceRateChartEnrollee | cdc | lastModifiedDateTime |
| BenefitInsuranceRateChartFixedAmount | cdc | lastModifiedDateTime |
| BenefitLeaveTravelReimbursementClaim | cdc | lastModifiedDateTime |
| BenefitLegalEntity | cdc | lastModifiedDateTime |
| BenefitLifeEventConfiguration | cdc | lastModifiedDateTime |
| BenefitOpenEnrollmentCycleConfiguration | cdc | lastModifiedDateTime |
| BenefitOverviewHyperlinkConfiguration | cdc | lastModifiedDateTime |
| BenefitOverviewHyperlinkDetails | cdc | lastModifiedDateTime |
| BenefitPaymentOptions | cdc | lastModifiedDateTime |
| BenefitPensionAdditionalContributionLimits | cdc | lastModifiedDateTime |
| BenefitPensionAdditionalEmployeeContributionDetail | cdc | lastModifiedDateTime |
| BenefitPensionDependentNominees | cdc | lastModifiedDateTime |
| BenefitPensionEmployeeContributionDetail | cdc | lastModifiedDateTime |
| BenefitPensionEmployerContributionDetail | cdc | lastModifiedDateTime |
| BenefitPensionEnrollmentContributionDetail | cdc | lastModifiedDateTime |
| BenefitPensionFund | cdc | lastModifiedDateTime |
| BenefitPensionFundEnrollmentContributionDetail | cdc | lastModifiedDateTime |
| BenefitPensionMinMaxContributionLimits | cdc | lastModifiedDateTime |
| BenefitPensionNonDependentNominees | cdc | lastModifiedDateTime |
| BenefitPensionStatutoryMinimumLookup | cdc | lastModifiedDateTime |
| BenefitProgram | cdc | lastModifiedDateTime |
| BenefitProgramEnrollment | cdc | lastModifiedDateTime |
| BenefitProgramEnrollmentDetail | cdc | lastModifiedDateTime |
| BenefitProgramExceptionDetails | cdc | lastModifiedDateTime |
| BenefitSavingsPlanCatchUpDetail | cdc | lastModifiedDateTime |
| BenefitSavingsPlanContingentBeneficiary | cdc | lastModifiedDateTime |
| BenefitSavingsPlanERContributionConfig | cdc | lastModifiedDateTime |
| BenefitSavingsPlanERContributionConfigDetail | cdc | lastModifiedDateTime |
| BenefitSavingsPlanEnrollmentContributionDetail | cdc | lastModifiedDateTime |
| BenefitSavingsPlanEnrollmentDetails | cdc | lastModifiedDateTime |
| BenefitSavingsPlanPrimaryBeneficiary | cdc | lastModifiedDateTime |
| BenefitSavingsPlanSubType | cdc | lastModifiedDateTime |
| BenefitSavingsPlanSubTypeCountryLookup | cdc | lastModifiedDateTime |
| BenefitSavingsPlanTierConfiguration | cdc | lastModifiedDateTime |
| BenefitSchedulePeriod | cdc | lastModifiedDateTime |
| BenefitSchedules | cdc | lastModifiedDateTime |
| BenefitsConfigUIScreenLookup | cdc | lastModifiedDateTime |
| BenefitsConfirmationStatementConfiguration | cdc | lastModifiedDateTime |
| BenefitsException | cdc | lastModifiedDateTime |
| BenefitsIntegrationOneTimeInfo | cdc | lastModifiedDateTime |
| BenefitsIntegrationRecurringInfo | cdc | lastModifiedDateTime |
| EmployeeWithEmployerMatchContributionEntries | cdc | lastModifiedDateTime |
| EmployeeWithEmployerMatchContributions | cdc | lastModifiedDateTime |
| IRSPremiumTable | cdc | lastModifiedDateTime |
| ImputedCostForAgeRanges | cdc | lastModifiedDateTime |
| InsuranceBenefitDetails | cdc | lastModifiedDateTime |
| InsuranceEnrollmentFieldsConfiguration | cdc | lastModifiedDateTime |
| LifeEventForBenefit | cdc | lastModifiedDateTime |
| PensionBandingConfiguration | cdc | lastModifiedDateTime |
| PensionBandingConfigurationDetails | cdc | lastModifiedDateTime |
| SavingsAccountBenefitDetails | cdc | lastModifiedDateTime |
| SavingsAccountDeductionDetails | cdc | lastModifiedDateTime |
| SavingsAccountTierConfiguration | cdc | lastModifiedDateTime |
| SavingsAccountUSA | cdc | lastModifiedDateTime |

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

**Get all records from BenefitEmployeeClaim:**
```http
GET https://{api-server}/odata/v2/BenefitEmployeeClaim
Authorization: Basic {base64_credentials}
Accept: application/json
```

**Get records modified after a date (for CDC):**
```http
GET https://{api-server}/odata/v2/BenefitEmployeeClaim?$filter=lastModifiedDateTime ge datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc
Authorization: Basic {base64_credentials}
Accept: application/json
```

**Get specific fields with pagination:**
```http
GET https://{api-server}/odata/v2/BenefitEmployeeClaim?$select=externalCode,createdDateTime,lastModifiedDateTime&$top=100&$skip=0&$inlinecount=allpages
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
          "uri": "https://{api-server}/odata/v2/BenefitEmployeeClaim('key')",
          "type": "SFOData.BenefitEmployeeClaim"
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

- **SAP SuccessFactors API Spec**: `ECGlobalBenefits.json`
- **SAP Help Portal**: [Global Benefits on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/c16959d69ff0443087bb5ae3a9176318.html)
- **SAP API Business Hub**: [SAP SuccessFactors APIs](https://api.sap.com/package/SuccessFactorsFoundation/overview)
- **Authentication Guide**: [SAP SuccessFactors OData API Authentication](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/5c8bca0af1654b05a83193b2922dcee2.html)
