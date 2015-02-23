package org.apache98.hadoop;

public class PackageNameFix {

	private static final String ORG_APACHE98 = "org\\.apache98\\.";
	private static final String ORG_APACHE98_ = ORG_APACHE98 + ".*";
	private static final String ORG_APACHE = "org\\.apache\\.";
	private static final String ORG_APACHE_ = ORG_APACHE + ".*";

	public static String add98(String className) {
		if (className == null) {
			return null;
		}
		if (className.matches(ORG_APACHE_)) {
			return className.replaceFirst(ORG_APACHE, ORG_APACHE98);
		}
		return className;
	}

	public static String remove98(String className) {
		if (className == null) {
			return null;
		}
		if (className.matches(ORG_APACHE98_)) {
			return className.replaceFirst(ORG_APACHE98, ORG_APACHE);
		}
		return className;
	}

}
