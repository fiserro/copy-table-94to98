package org.apache98.hadoop;

public class PackageNameFix {

	public static String add98(String className) {
		if (className == null) {
			return null;
		}
		return className.replaceFirst("org\\.apache\\.", "org\\.apache98\\.");
	}

	public static String remove98(String className) {
		if (className == null) {
			return null;
		}
		return className.replaceFirst("org\\.apache98\\.", "org\\.apache\\.");
	}

}
