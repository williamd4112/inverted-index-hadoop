package calculateAverage;

public class CompareUtil {
	public static int compare(String s1, String s2)
	{
        int i;
        for(i = 0; i < s1.length() && i < s2.length(); i++)
        {
                int c1 = (Character.isLowerCase(s1.charAt(i))) ?
                                (Character.toUpperCase(s1.charAt(i)) - 'A') * 2 :
                                (s1.charAt(i) - 'A') * 2 - 1;
                int c2 = (Character.isLowerCase(s2.charAt(i))) ?
                                (Character.toUpperCase(s2.charAt(i)) - 'A') * 2 :
                                (s2.charAt(i) - 'A') * 2 - 1;
                if(c1 < c2) return -1;
                else if(c1 > c2) return 1;
                else continue;
        }

        if(s1.length() < s2.length())
                return -1;
        else if(s1.length() > s2.length())
                return 1;
        else
                return 0;
	}
}
