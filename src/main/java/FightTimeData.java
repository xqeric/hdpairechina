/**
 * Created by qiang on 17-10-3.
 */


import org.apache.hadoop.io.WritableComparable;

public class FightTimeData {

    public int year;

    public int month;

    public int day;

    public int dayOfWeek;

    /**
     * flight num
     */
    public String flightNum;

    /**
     * origin from
     */
    public String Origin;

    /**
     * destination
     */
    public String Dest;

    /**
     * arrive delay
     */
    public int arrDelay = 0;

    /**
     * depart delay
     */
    public int depDelay = 0;




}
