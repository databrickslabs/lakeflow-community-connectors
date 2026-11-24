# SAP SuccessFactors — Connector Reference

This connector exposes 250 SAP SuccessFactors OData entity sets as logical
tables. Authentication and pipeline setup are documented in `README.md`;
detailed per-module field references live under `api_docs/`.

## Authentication

SAP SuccessFactors uses HTTP Basic authentication over OData v2:

```
Authorization: Basic base64("<username>@<companyId>:<password>")
```

Credentials are passed via the `username` and `password` connection
parameters; the `endpoint_url` parameter selects the data center
(`https://api.successfactors.eu`, `https://api.successfactors.com`, etc.).

## OData v2 conventions

- Base URL: `<endpoint_url>/odata/v2`
- Pagination: standard `__next` link in responses, `$skip` + `$top` for
  positional pagination.
- Cursor field for incremental tables: `lastModifiedDateTime` (where the
  module exposes it).

## Connector tables

The 250 tables below are returned by `list_tables()`. Each maps to a single
OData entity set under `<endpoint_url>/odata/v2/<EntitySet>`. For full field
schemas and module-specific notes, consult the per-module references in the
`api_docs/` directory (e.g. `api_docs/ECEmploymentInformation_api_doc.md`).

### Tables

- `Advance`
- `AdvancesAccumulation`
- `AdvancesEligibility`
- `AdvancesInstallments`
- `AssignedComplianceForm`
- `Attachment`
- `Bank`
- `Benefit`
- `BudgetGroup`
- `CalibrationSession`
- `CalibrationSessionSubject`
- `CalibrationSubjectRank`
- `CalibrationTemplate`
- `Candidate`
- `CandidateBackground_Certificates`
- `CandidateBackground_Education`
- `CandidateBackground_InsideWorkExperience`
- `CandidateBackground_Languages`
- `CandidateBackground_OutsideWorkExperience`
- `CandidateBackground_TalentPool`
- `CandidateBackground_TalentPoolcorp`
- `CandidateComments`
- `CandidateLight`
- `CompetencyEntity`
- `ComplianceDocumentFlow`
- `ComplianceFormData`
- `ComplianceFormSignature`
- `ComplianceProcess`
- `ComplianceProcessTask`
- `Country`
- `CurrencyConversion`
- `CustomPayType`
- `CustomPayTypeAssignment`
- `DGExpression`
- `DGField`
- `DGFieldOperator`
- `DGFieldValue`
- `DGFilter`
- `DGPeoplePool`
- `DeductionScreenId`
- `DevLearningCertifications`
- `DevLearning_4201`
- `DynamicGroup`
- `DynamicGroupDefinition`
- `EMEvent`
- `EMEventAttribute`
- `EMEventPayload`
- `EMMonitoredProcess`
- `EmpBeneficiary`
- `EmpCompensation`
- `EmpCompensationCalculated`
- `EmpCompensationGroupSumCalculated`
- `EmpCostAssignment`
- `EmpEmployment`
- `EmpEmploymentHigherDuty`
- `EmpEmploymentTermination`
- `EmpJob`
- `EmpJobRelationships`
- `EmpPayCompNonRecurring`
- `EmpPayCompRecurring`
- `EmpPensionPayout`
- `EmpWorkPermit`
- `EmployeeCompensation`
- `EmployeeDismissalProtection`
- `EmployeeDismissalProtectionDetail`
- `EmployeePayrollRunResults`
- `EmployeePayrollRunResultsItems`
- `ExternalUser`
- `FOBusinessUnit`
- `FOCompany`
- `FOCorporateAddressDEFLT`
- `FOCostCenter`
- `FODepartment`
- `FODivision`
- `FODynamicRole`
- `FOEventReason`
- `FOFrequency`
- `FOGeozone`
- `FOJobClassLocalAUS`
- `FOJobClassLocalBRA`
- `FOJobClassLocalCAN`
- `FOJobClassLocalDEFLT`
- `FOJobClassLocalFRA`
- `FOJobClassLocalGBR`
- `FOJobClassLocalITA`
- `FOJobClassLocalUSA`
- `FOJobCode`
- `FOJobFunction`
- `FOLocation`
- `FOLocationGroup`
- `FOPayComponent`
- `FOPayComponentGroup`
- `FOPayGrade`
- `FOPayGroup`
- `FOPayRange`
- `HireDateChange`
- `InterviewIndividualAssessment`
- `InterviewOverallAssessment`
- `JobApplication`
- `JobApplicationAssessmentOrder`
- `JobApplicationAssessmentReport`
- `JobApplicationAssessmentReportDetail`
- `JobApplicationAudit`
- `JobApplicationBackgroundCheckRequest`
- `JobApplicationBackgroundCheckResult`
- `JobApplicationComments`
- `JobApplicationInterview`
- `JobApplicationOnboardingData`
- `JobApplicationOnboardingStatus`
- `JobApplicationQuestionResponse`
- `JobApplicationSnapshot_Certificates`
- `JobApplicationSnapshot_Education`
- `JobApplicationSnapshot_InsideWorkExperience`
- `JobApplicationSnapshot_Languages`
- `JobApplicationSnapshot_OutsideWorkExperience`
- `JobApplicationSnapshot_TalentPool`
- `JobApplicationSnapshot_TalentPoolcorp`
- `JobApplicationStatus`
- `JobApplicationStatusAuditTrail`
- `JobApplicationStatusLabel`
- `JobOffer`
- `JobOfferApprover`
- `JobReqFwdCandidates`
- `JobReqQuestion`
- `JobReqScreeningQuestion`
- `JobReqScreeningQuestionChoice`
- `JobRequisition`
- `JobRequisitionGroupOperator`
- `JobRequisitionLocale`
- `JobRequisitionOperator`
- `JobRequisitionPosting`
- `JourneyDetails`
- `LegacyPositionEntity`
- `LocalizedData`
- `MDFEnumValue`
- `MDFLocalizedValue`
- `NominationTarget`
- `NomineeHistory`
- `ONB2EquipmentActivity`
- `ONB2EquipmentType`
- `ONB2EquipmentTypeValue`
- `ONB2Process`
- `ONB2ProcessTask`
- `ONB2ProcessTrigger`
- `OfferLetter`
- `OnboardingCandidateInfo`
- `OnboardingEquipment`
- `OnboardingEquipmentActivity`
- `OnboardingEquipmentType`
- `OnboardingEquipmentTypeValue`
- `OnboardingGoal`
- `OnboardingGoalActivity`
- `OnboardingGoalCategory`
- `OnboardingMeetingActivity`
- `OnboardingMeetingEvent`
- `OnboardingNewHireActivitiesStep`
- `OnboardingProcess`
- `OneTimeDeduction`
- `PaymentInformationDetailV3`
- `PaymentInformationDetailV3USA`
- `PaymentInformationV3`
- `PaymentMethodAssignmentV3`
- `PaymentMethodV3`
- `PersonEmpTerminationInfo`
- `Photo`
- `PickListV2`
- `PickListValueV2`
- `PicklistOption`
- `RBPBasicPermission`
- `RBPRole`
- `RBPRule`
- `RCMAdminReassignOfferApprover`
- `RcmCompetency`
- `RecurringDeduction`
- `RecurringDeductionItem`
- `ScimGroup`
- `ScimUser`
- `Successor`
- `TalentGraphicOption`
- `TalentPool`
- `TalentRatings`
- `Territory`
- `User`
- `UserPermissions`
- `achievement`
- `activity`
- `budget_period`
- `calibration_competency_rating`
- `calibration_executive_reviewer`
- `calibration_rating`
- `calibration_rating_option`
- `calibration_session`
- `calibration_session_reviewer`
- `calibration_subject`
- `calibration_subject_comment`
- `clock_in_clock_out_groups`
- `continuous_performance_user_permission`
- `cpm_achievements`
- `cpm_activities`
- `cpm_activity_status`
- `cpm_activity_updates`
- `cpm_user_permissions`
- `cust_CommutingAllowance`
- `cust_ProgressiveDisciplinaryAction`
- `cust_auth_sign`
- `cust_grievances`
- `cust_voluntarySeparationRequest`
- `custom_nav`
- `custom_tasks`
- `extension_point_task`
- `feedback`
- `feedback_requests`
- `form360_participant_detail`
- `form_competency`
- `form_customized_weighted_rating_section`
- `form_folder`
- `form_header`
- `form_objective`
- `form_perf_pot_summary_section`
- `form_review_feedback`
- `form_template`
- `functional_area`
- `fund`
- `fund_center`
- `goal_1`
- `goal_comment_1`
- `goal_permission_1`
- `goal_plan_state`
- `goal_plan_template`
- `goal_task_1`
- `goal_weight`
- `grant`
- `i9_audit_trail_record`
- `pbc_employee_financing`
- `pbc_employee_grouping`
- `pbc_symbolic_account`
- `project_controlling_object`
- `review_route_map`
- `scim_group`
- `scim_user`
- `success_store_content`
- `success_store_content_blob`
- `supporter_feedback`
- `theme_config`
- `theme_info`
- `theme_template`
- `time_event_types`
- `todo`
- `todo_entry_v2`
- `user_capabilities`


## Module-level references

The following module-aligned reference files exist under `api_docs/` with
detailed field schemas and module-specific behavior:

- [`AdditionalServices_api_doc.md`](api_docs/)
- [`AvailableTimeTypes_api_doc.md`](api_docs/)
- [`Balances_api_doc.md`](api_docs/)
- [`BudgetPeriodGO_api_doc.md`](api_docs/)
- [`CalSession_api_doc.md`](api_docs/)
- [`ClockInClockOutExternal_api_doc.md`](api_docs/)
- [`ClockInClockOutIntegration_api_doc.md`](api_docs/)
- [`ClockInClockOutTimeEventsRestAPI_api_doc.md`](api_docs/)
- [`ClockInClockOut_api_doc.md`](api_docs/)
- [`ECAdvances_api_doc.md`](api_docs/)
- [`ECCompensationInformation_api_doc.md`](api_docs/)
- [`ECDismissalProtection_api_doc.md`](api_docs/)
- [`ECEmployeeCentralPayroll_api_doc.md`](api_docs/)
- [`ECEmploymentInformation_api_doc.md`](api_docs/)
- [`ECFoundationOrganization_api_doc.md`](api_docs/)
- [`ECGlobalBenefits_api_doc.md`](api_docs/)
- [`ECPaymentInformation_api_doc.md`](api_docs/)
- [`ECSkillsManagement_api_doc.md`](api_docs/)
- [`EmpCostAssignment_api_doc.md`](api_docs/)
- [`ExtensionPoint_api_doc.md`](api_docs/)
- [`FoundationPlatformPLT_api_doc.md`](api_docs/)
- [`FunctionalAreaGO_api_doc.md`](api_docs/)
- [`FundCenterGO_api_doc.md`](api_docs/)
- [`InstructionalText_api_doc.md`](api_docs/)
- [`OnboardingOBX_api_doc.md`](api_docs/)
- [`OnboardingONB_api_doc.md`](api_docs/)
- [`PLTCustomNavigation_api_doc.md`](api_docs/)
- [`PLTDPCS_api_doc.md`](api_docs/)
- [`PLTExecutionManager_api_doc.md`](api_docs/)
- [`PLTGenericObjects_api_doc.md`](api_docs/)
- [`PLTRoleBasedPermissions_api_doc.md`](api_docs/)
- [`PLTSSO_api_doc.md`](api_docs/)
- [`PLTScim_api_doc.md`](api_docs/)
- [`PLTSuccessStore_api_doc.md`](api_docs/)
- [`PLTTodo_api_doc.md`](api_docs/)
- [`PLTUserInterfaceThemes_api_doc.md`](api_docs/)
- [`PLTUserManagement_api_doc.md`](api_docs/)
- [`PMFormsManagement_api_doc.md`](api_docs/)
- [`PMGMContinuousFeedback_api_doc.md`](api_docs/)
- [`PMGMContinuousPerformanceManagement_api_doc.md`](api_docs/)
- [`PMGMContinuousPerformanceREST_api_doc.md`](api_docs/)
- [`PMGMMultirater_api_doc.md`](api_docs/)
- [`PMGMPerformanceREST_api_doc.md`](api_docs/)
- [`PerformanceandGoalsPMGM_api_doc.md`](api_docs/)
- [`ProjectControllingObject_api_doc.md`](api_docs/)
- [`RCMCandidate_api_doc.md`](api_docs/)
- [`RCMJobApplication_api_doc.md`](api_docs/)
- [`RCMJobRequisition_api_doc.md`](api_docs/)
- [`RCMOffer_api_doc.md`](api_docs/)
- [`RecruitingRCM_api_doc.md`](api_docs/)
- [`SCMNominationService_api_doc.md`](api_docs/)
- [`SDCalibration_api_doc.md`](api_docs/)
- [`SDSuccessionManagement_api_doc.md`](api_docs/)
- [`SuccessionandDevelopmentSD_api_doc.md`](api_docs/)
- [`TimeOffEvents_api_doc.md`](api_docs/)
- [`UploadAttachment_api_doc.md`](api_docs/)
- [`UserManagementSCIM_api_doc.md`](api_docs/)
- [`i9audittrail_api_doc.md`](api_docs/)
- [`sap-sf-EmpEmploymentHigherDuty-v1_api_doc.md`](api_docs/)
- [`sap-sf-FMLARequest-v1_api_doc.md`](api_docs/)
- [`sap-sf-FundGO-v1_api_doc.md`](api_docs/)
- [`sap-sf-GrantGO-v1_api_doc.md`](api_docs/)
- [`sap-sf-PLTRBPGenAI-v1_api_doc.md`](api_docs/)
- [`sap-sf-PositionBudgetingControl-v1_api_doc.md`](api_docs/)
- [`sap-sf-customTasks-v2_api_doc.md`](api_docs/)
- [`sap-sf-employeeCompensation-v1_api_doc.md`](api_docs/)
- [`sap-sf-newhirejourney-v1_api_doc.md`](api_docs/)
