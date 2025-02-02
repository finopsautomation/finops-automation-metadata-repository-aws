package finopsautomation.metadata.repository.aws;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;

import finopsautomation.metadata.model.ProviderTypeEnum;
import finopsautomation.metadata.model.account.AccountDefinition;
import finopsautomation.metadata.repository.BaseAccountMetadataRepository;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/**
 * Persistence of Account Metadata within AWS DynamoDB
 */
@Profile("storage-aws-ddb")
@Repository
public class AWSDDBAccountMetadataRepository extends BaseAccountMetadataRepository {
	private static final String ACCOUNT_TABLE_NAME = "AccountMetadata";
	
	private static final String PRIMARY_KEY_ATTRIBUTE = "pk";
	private static final String ACCOUNT_NAME_ATTRIBUTE = "accountName";
	private static final String BILLING_ACCOUNT_ID_ATTRIBUTE = "billingAccountID";
	private static final String FRIENDLY_ACCOUNT_NAME_ATTRIBUTE = "friendlyAccountName";
	private static final String PROVIDER_TYPE_ATTRIBUTE = "providerType";
	private static final String LINKED_ACCOUNT_ID_ATTRIBUTE = "linkedAccountID";
	private static final String PROVISION_DATE_ATTRIBUTE = "provisionDate";
	private static final String DECOMMISSION_DATE_ATTRIBUTE = "decommissionDate";
	private static final String ACCOUNT_NOTES_ATTRIBUTE = "accountNotes";
	private static final String TECHNICAL_CONTACT_NAME_ATTRIBUTE = "technicalContactName";
	private static final String TECHNICAL_CONTACT_EMAIL_ADDRESS_ATTRIBUTE = "tTechnicalContactEmailAddress";
	private static final String BUSINESS_CONTACT_NAME_ATTRIBUTE = "businessContactName";
	private static final String BUSINESS_CONTACT_EMAIL_ADDRESS_ATTRIBUTE = "businessContactEmailAddress";
	
	@Override
	public <S extends AccountDefinition> S save(S definition) {
		try (DynamoDbClient client = DynamoDbClient.create()) {
			Map<String, AttributeValue> attributes = toAttributes(definition);
			
			PutItemRequest request = PutItemRequest.builder().tableName(ACCOUNT_TABLE_NAME).item(attributes).build();
			
			PutItemResponse resp = client.putItem(request);
			// TODO: Add logic to check response here
		}
		
		return definition;
	}
	
	@Override
	public Optional<AccountDefinition> findById(String accountID) {
		Optional<AccountDefinition> result = Optional.empty();

		try (DynamoDbClient client = DynamoDbClient.create()) {
			Map<String, AttributeValue> attributes = new TreeMap<String, AttributeValue>();
			attributes.put(":pkval", AttributeValue.builder().s(accountID).build());
			
			QueryRequest request = QueryRequest.builder().tableName(ACCOUNT_TABLE_NAME).keyConditionExpression("pk = :pkval").expressionAttributeValues(attributes).build();
			
			QueryResponse resp = client.query(request);
			if (resp.hasItems() && resp.items().size() == 1) {
				Map<String, AttributeValue> itemAttributes = resp.items().get(0);
				
				AccountDefinition billing = fromAttributes(itemAttributes);
				
				result = Optional.of(billing);
			}
		}

		return result;
	}

	@Override
	public boolean existsById(String id) {
		return findById(id).isPresent();
	}

	@Override
	public long count() {
		return findAll().size();
	}

	@Override
	public List<AccountDefinition> findAll() {
		List<AccountDefinition> result = new ArrayList<AccountDefinition>();
		
		try (DynamoDbClient client = DynamoDbClient.create()) {
			ScanRequest request = ScanRequest.builder().tableName(ACCOUNT_TABLE_NAME).build();

			ScanResponse resp = client.scan(request);
			if (resp.hasItems()) {
				for(Map<String, AttributeValue> item : resp.items()) {
					AccountDefinition definition = fromAttributes(item);
					
					result.add(definition);
				}
			}
		}
		
		return result;
	}
	
	/**
	 * Convert the AccountDefinition to a Map of DDB Attributes
	 * 
	 * @param source Account Definition (required)
	 * 
	 * @return Map of AttributeValues
	 */
	private Map<String, AttributeValue> toAttributes(AccountDefinition source) {
		Map<String, AttributeValue> target = new TreeMap<String, AttributeValue>();
		
		// Required attributes
		target.put(PRIMARY_KEY_ATTRIBUTE, AttributeValue.builder().s(source.getAccountID()).build());
		
		target.put(ACCOUNT_NAME_ATTRIBUTE, AttributeValue.builder().s(source.getAccountName()).build());
		target.put(FRIENDLY_ACCOUNT_NAME_ATTRIBUTE, AttributeValue.builder().s(source.getFriendlyAccountName()).build());
		target.put(BILLING_ACCOUNT_ID_ATTRIBUTE, AttributeValue.builder().s(source.getBillingAccountID()).build());
		target.put(PROVIDER_TYPE_ATTRIBUTE, AttributeValue.builder().s(source.getProviderType().name()).build());
		
		// Optional attributes
		if (source.getLinkedAccountID() != null) {
			target.put(LINKED_ACCOUNT_ID_ATTRIBUTE, AttributeValue.builder().s(source.getLinkedAccountID()).build());
		}
		if (source.getAccountNotes() != null) {
			target.put(ACCOUNT_NOTES_ATTRIBUTE, AttributeValue.builder().s(source.getAccountNotes()).build());
		}
		if (source.getTechnicalContactName() != null) {
			target.put(TECHNICAL_CONTACT_NAME_ATTRIBUTE, AttributeValue.builder().s(source.getTechnicalContactName()).build());
		}
		if (source.getTechnicalContactEmailAddress() != null) {
			target.put(TECHNICAL_CONTACT_EMAIL_ADDRESS_ATTRIBUTE, AttributeValue.builder().s(source.getTechnicalContactEmailAddress()).build());
		}
		if (source.getBusinessContactName() != null) {
			target.put(BUSINESS_CONTACT_NAME_ATTRIBUTE, AttributeValue.builder().s(source.getBusinessContactName()).build());
		}
		if (source.getBusinessContactEmailAddress() != null) {
			target.put(BUSINESS_CONTACT_EMAIL_ADDRESS_ATTRIBUTE, AttributeValue.builder().s(source.getBusinessContactEmailAddress()).build());
		}
		if (source.getProvisionDate() != null) {
			target.put(PROVISION_DATE_ATTRIBUTE, AttributeValue.builder().s(source.getProvisionDate().toString()).build());
		}
		if (source.getDecommissionDate() != null) {
			target.put(DECOMMISSION_DATE_ATTRIBUTE, AttributeValue.builder().s(source.getDecommissionDate().toString()).build());
		}
		
		return target;
	}	

	/**
	 * Convert from a Map of DDB Attributes to a AccountDefinition
	 * 
	 * @param source Map of DDB Attributes (required)
	 *  
	 * @return AccounteDefinition
	 */
	private AccountDefinition fromAttributes(Map<String, AttributeValue> source) {
		String accountID = source.get(PRIMARY_KEY_ATTRIBUTE).s();
				
		AccountDefinition target = new AccountDefinition(accountID);

		// Required attributes
		target.setProviderType(ProviderTypeEnum.valueOf(source.get(PROVIDER_TYPE_ATTRIBUTE).s()));
		target.setAccountName(source.get(ACCOUNT_NAME_ATTRIBUTE).s());
		target.setFriendlyAccountName(source.get(FRIENDLY_ACCOUNT_NAME_ATTRIBUTE).s());
		target.setBillingAccountID(source.get(BILLING_ACCOUNT_ID_ATTRIBUTE).s());
		
		// Optional attributes
		if (source.containsKey(LINKED_ACCOUNT_ID_ATTRIBUTE)) {
			target.setLinkedAccountID(source.get(LINKED_ACCOUNT_ID_ATTRIBUTE).s());
		}
		if (source.containsKey(ACCOUNT_NOTES_ATTRIBUTE)) {
			target.setAccountNotes(source.get(ACCOUNT_NOTES_ATTRIBUTE).s());
		}
		if (source.containsKey(TECHNICAL_CONTACT_NAME_ATTRIBUTE)) {
			target.setTechnicalContactName(source.get(TECHNICAL_CONTACT_NAME_ATTRIBUTE).s());
		}
		if (source.containsKey(TECHNICAL_CONTACT_EMAIL_ADDRESS_ATTRIBUTE)) {
			target.setTechnicalContactEmailAddress(source.get(TECHNICAL_CONTACT_EMAIL_ADDRESS_ATTRIBUTE).s());
		}
		if (source.containsKey(BUSINESS_CONTACT_NAME_ATTRIBUTE)) {
			target.setBusinessContactName(source.get(BUSINESS_CONTACT_NAME_ATTRIBUTE).s());
		}
		if (source.containsKey(BUSINESS_CONTACT_EMAIL_ADDRESS_ATTRIBUTE)) {
			target.setBusinessContactEmailAddress(source.get(BUSINESS_CONTACT_EMAIL_ADDRESS_ATTRIBUTE).s());
		}
		if (source.containsKey(PROVISION_DATE_ATTRIBUTE)) {
			target.setProvisionDate(LocalDate.parse(source.get(PROVISION_DATE_ATTRIBUTE).s()));
		}
		if (source.containsKey(DECOMMISSION_DATE_ATTRIBUTE)) {
			target.setDecommissionDate(LocalDate.parse(source.get(DECOMMISSION_DATE_ATTRIBUTE).s()));
		}
		
		return target;
	}
}
