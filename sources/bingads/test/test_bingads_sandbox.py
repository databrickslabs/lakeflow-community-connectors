# Simple Bing Ads SDK Sandbox Test
# Run manually in debug mode - no functions, just simple code
#
# Prerequisites:
# 1. Register an app in Azure Portal (see get_refresh_token.py for instructions)
# 2. Run get_refresh_token.py to obtain your refresh token
# 3. Set CLIENT_ID and REFRESH_TOKEN below
#
# Reference: https://learn.microsoft.com/en-us/advertising/guides/get-started-python?view=bingads-13

from bingads.service_client import ServiceClient
from bingads.authorization import AuthorizationData, OAuthDesktopMobileAuthCodeGrant

# === SANDBOX CONFIGURATION ===
DEVELOPER_TOKEN = "BBD37VB98"  # Universal sandbox developer token
CLIENT_ID = "1d0b0336-4189-46ce-bf2c-a1caf4c00e0b"  # Your Application (client) ID from Azure Portal
REFRESH_TOKEN = "M.C516_BL2.0.U.-CqZ4MMMrkGpLTpl1ex!Mq9iIYFgXWMSoUGhi0*Mlh12HN6PrNgwlYIZzNMguA7wz5j0RYxMUkdBmYQzO8S5RCvLP*ZhTMHrFOY9JEXzDjw4xxlkpGfvcyet1sbdntwFS2!URfO*Zn4DGlO920WY1lFunCHQLpj*CmYC7p*K6gFkyFQTmb7cy2JEXSNTnDuvYl43wOOd2izHNFKvntl*Xt!cEzCbG0Q1ZkicMiq4lo5!pBpj2Jjpte9Mub!NYi2xYiE3HveCVOEW9EoFnFrgZxj4qWkfvfgYu8mZYc01sw9ItUwMdMcWiu1DxpqHOAOM8Y0*BmT*r7hpbJW1GBj*7Y805YxEiLZwz!gnzVxw*ldElyLbdyicSQdLSI8ObGzpcYvSQEH6xMcNwklllDTiZJtDlsaMqefkWYHsFVVzDrjbYF0BafzkFOmRZJUjFeCchB5vgNbEgJ8aK8cWUtV5n!gAZaQrqWY9IWOQ2MaICos57dClJFHL7N5hoDlrBIJxI!g$$"  # Get this by running get_refresh_token.py
ENVIRONMENT = "sandbox"

# === SETUP AUTHORIZATION ===
authorization_data = AuthorizationData(
    account_id=None,
    customer_id=None,
    developer_token=DEVELOPER_TOKEN,
    authentication=None,
)

authentication = OAuthDesktopMobileAuthCodeGrant(
    client_id=CLIENT_ID,
    env=ENVIRONMENT,
)

authentication.request_oauth_tokens_by_refresh_token(REFRESH_TOKEN)
authorization_data.authentication = authentication
print("✓ Authentication successful")

# === TEST API CALL - Get Current User ===
customer_service = ServiceClient(
    service="CustomerManagementService",
    version=13,
    authorization_data=authorization_data,
    environment=ENVIRONMENT,
)

response = customer_service.GetUser(UserId=None)

print(f"✓ API call successful")
print(f"  User ID: {response.User.Id}")
print(f"  User Name: {response.User.UserName}")
print(f"  Contact Email: {response.User.ContactInfo.Email}")

# === TEST API CALL - Get Campaigns ===
accounts_response = customer_service.SearchAccounts(
    PageInfo={"Index": 0, "Size": 10},
    Predicates={"Predicate": [{"Field": "UserId", "Operator": "Equals", "Value": response.User.Id}]},
)

if accounts_response.AdvertiserAccount:
    account = accounts_response.AdvertiserAccount[0]
    authorization_data.account_id = account.Id
    authorization_data.customer_id = account.ParentCustomerId
    print(f"\n✓ Found account ID: {account.Id}, Customer ID: {account.ParentCustomerId}")

    campaign_service = ServiceClient(
        service="CampaignManagementService",
        version=13,
        authorization_data=authorization_data,
        environment=ENVIRONMENT,
    )

    campaigns_response = campaign_service.GetCampaignsByAccountId(
        AccountId=account.Id,
        CampaignType="Search",
    )

    print(f"✓ GetCampaignsByAccountId call successful")
    if hasattr(campaigns_response, 'Campaign') and campaigns_response.Campaign:
        for campaign in campaigns_response.Campaign:
            print(f"  Campaign ID: {campaign.Id}, Name: {campaign.Name}")
    else:
        print("  No campaigns found for this account")
else:
    print("\n⚠ No accounts found - skipping GetCampaignsByAccountId test")
