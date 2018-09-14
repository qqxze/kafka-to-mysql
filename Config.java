package com.database.kafka.ExtractData;

public class Config {
	// JDBC 椹卞姩鍚嶅強鏁版嵁搴� URL
	public static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	public static final String DB_URL = "jdbc:mysql://172.31.42.214:3306/warehouse";
	//public static final String DB_URL = "jdbc:mysql://192.168.56.122:3306/UserBehavior?userUnicode=true&amp;characterEncoding=UTF-8 &amp;zeroDateTimeBehavior=converToNull";

	// 鏁版嵁搴撶殑鐢ㄦ埛鍚嶄笌瀵嗙爜锛岄渶瑕佹牴鎹嚜宸辩殑璁剧疆
	public static final String USER = "root";
	public static final String PASS = "cluster";

	public static final String articleInfoSQL = "insert into articleinfo values(?, ?, ?, ?, ?, ?, ?, ?)";
	public static final String userBasicSQL = "insert into userbasic values(?, ?, ?, ?, ?, ?)";
	public static final String userEduSQL = "insert into useredu values(?, ?, ?, ?)";
	public static final String userSkillSQL = "insert into userskill values(?, ?)";
	public static final String userInterestSQL = "insert into userinterest values(?, ?)";
	public static final String userBehaviorSQL = "insert into behavior values(?, ?, ?, ?)";

	public static enum Topics {
		USERBASIC("userbasic", userBasicSQL), USEREDU("useredu", userEduSQL), USERSKILL("userskill",
				userSkillSQL), USERINTEREST("userinterest", userInterestSQL), USERBEHAVIOR("userbehavior",
						userBehaviorSQL), ARTICLEINFO("articleinfo", articleInfoSQL);
		// 瀹氫箟绉佹湁鍙橀噺
		public  String topicName;
		public String insertSQL;

		// 鏋勯�犲嚱鏁帮紝鏋氫妇绫诲瀷鍙兘涓虹鏈�
		private Topics(String topicName, String insertSQL) {
			this.topicName = topicName;
			this.insertSQL = insertSQL;
		}
		
	}
}
