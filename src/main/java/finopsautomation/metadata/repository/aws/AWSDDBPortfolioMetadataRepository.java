package finopsautomation.metadata.repository.aws;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;

import finopsautomation.metadata.model.portfolio.PortfolioDefinition;
import finopsautomation.metadata.repository.BasePortfolioMetadataRepository;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/**
 * Persistence of Portfolio Metadata within AWS DynamoDB
 */
@Profile("storage-aws-ddb")
@Repository
public class AWSDDBPortfolioMetadataRepository extends BasePortfolioMetadataRepository {
	private static final String PORTFOLIO_TABLE_NAME = "PortfolioMetadata";
	
	private static final String PRIMARY_KEY_ATTRIBUTE = "pk";
	private static final String PORTFOLIO_NAME_ATTRIBUTE = "portfolioName";
	private static final String START_DATE_ATTRIBUTE = "startDate";
	private static final String END_DATE_ATTRIBUTE = "endDate";
	private static final String PORTFOLIO_NOTES_ATTRIBUTE = "portfolioNotes";
	private static final String TECHNICAL_CONTACT_NAME_ATTRIBUTE = "technicalContactName";
	private static final String TECHNICAL_CONTACT_EMAIL_ADDRESS_ATTRIBUTE = "tTechnicalContactEmailAddress";
	private static final String BUSINESS_CONTACT_NAME_ATTRIBUTE = "businessContactName";
	private static final String BUSINESS_CONTACT_EMAIL_ADDRESS_ATTRIBUTE = "businessContactEmailAddress";
	
	@Override
	public <S extends PortfolioDefinition> S save(S definition) {
		try (DynamoDbClient client = DynamoDbClient.create()) {
			Map<String, AttributeValue> attributes = toAttributes(definition);
			
			PutItemRequest request = PutItemRequest.builder().tableName(PORTFOLIO_TABLE_NAME).item(attributes).build();
			
			PutItemResponse resp = client.putItem(request);
			// TODO: Add logic to check response here
		}
		
		return definition;
	}
	
	@Override
	public Optional<PortfolioDefinition> findById(String accountID) {
		Optional<PortfolioDefinition> result = Optional.empty();

		try (DynamoDbClient client = DynamoDbClient.create()) {
			Map<String, AttributeValue> attributes = new TreeMap<String, AttributeValue>();
			attributes.put(":pkval", AttributeValue.builder().s(accountID).build());
			
			QueryRequest request = QueryRequest.builder().tableName(PORTFOLIO_TABLE_NAME).keyConditionExpression("pk = :pkval").expressionAttributeValues(attributes).build();
			
			QueryResponse resp = client.query(request);
			if (resp.hasItems() && resp.items().size() == 1) {
				Map<String, AttributeValue> itemAttributes = resp.items().get(0);
				
				PortfolioDefinition definition = fromAttributes(itemAttributes);
				
				result = Optional.of(definition);
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
	public List<PortfolioDefinition> findAll() {
		List<PortfolioDefinition> result = new ArrayList<PortfolioDefinition>();
		
		try (DynamoDbClient client = DynamoDbClient.create()) {
			ScanRequest request = ScanRequest.builder().tableName(PORTFOLIO_TABLE_NAME).build();

			ScanResponse resp = client.scan(request);
			if (resp.hasItems()) {
				for(Map<String, AttributeValue> item : resp.items()) {
					PortfolioDefinition definition = fromAttributes(item);
					
					result.add(definition);
				}
			}
		}
		
		return result;
	}
	
	/**
	 * Convert the PortfolioDefinition to a Map of DDB Attributes
	 * 
	 * @param source Portfolio Definition (required)
	 * 
	 * @return Map of AttributeValues
	 */
	private Map<String, AttributeValue> toAttributes(PortfolioDefinition source) {
		Map<String, AttributeValue> target = new TreeMap<String, AttributeValue>();
		
		// Required attributes
		target.put(PRIMARY_KEY_ATTRIBUTE, AttributeValue.builder().s(source.getPortfolioID()).build());
		
		target.put(PORTFOLIO_NAME_ATTRIBUTE, AttributeValue.builder().s(source.getPortfolioName()).build());
		
		// Optional attributes
		if (source.getPortfolioNotes() != null) {
			target.put(PORTFOLIO_NOTES_ATTRIBUTE, AttributeValue.builder().s(source.getPortfolioNotes()).build());
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
		if (source.getStartDate() != null) {
			target.put(START_DATE_ATTRIBUTE, AttributeValue.builder().s(source.getStartDate().toString()).build());
		}
		if (source.getEndDate() != null) {
			target.put(END_DATE_ATTRIBUTE, AttributeValue.builder().s(source.getEndDate().toString()).build());
		}
		
		return target;
	}	

	/**
	 * Convert from a Map of DDB Attributes to a PortfolioDefinition
	 * 
	 * @param source Map of DDB Attributes (required)
	 *  
	 * @return PortfolioDefinition
	 */
	private PortfolioDefinition fromAttributes(Map<String, AttributeValue> source) {
		String portfolioID = source.get(PRIMARY_KEY_ATTRIBUTE).s();
				
		PortfolioDefinition target = new PortfolioDefinition(portfolioID);

		// Required attributes
		target.setPortfolioName(source.get(PORTFOLIO_NAME_ATTRIBUTE).s());
		
		// Optional attributes
		if (source.containsKey(PORTFOLIO_NOTES_ATTRIBUTE)) {
			target.setPortfolioNotes(source.get(PORTFOLIO_NOTES_ATTRIBUTE).s());
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
		if (source.containsKey(START_DATE_ATTRIBUTE)) {
			target.setStartDate(LocalDate.parse(source.get(START_DATE_ATTRIBUTE).s()));
		}
		if (source.containsKey(END_DATE_ATTRIBUTE)) {
			target.setEndDate(LocalDate.parse(source.get(END_DATE_ATTRIBUTE).s()));
		}
		
		return target;
	}
}
