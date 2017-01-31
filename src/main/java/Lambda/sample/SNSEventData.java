package Lambda.sample;

public class SNSEventData {
	private String recordId;
	private String locationId;
	private String tableName;

    public SNSEventData(String recordId, String locationId, String tableName) {
        this.recordId = recordId;
        this.locationId=locationId;
        this.tableName=tableName;
    }

    public String getRecordId() {
		return recordId;
	}

	public void setRecordId(String recordId) {
		this.recordId = recordId;
	}

	public String getLocationId() {
		return locationId;
	}

	public void setLocationId(String locationId) {
		this.locationId = locationId;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public SNSEventData() {
    }

    
}
