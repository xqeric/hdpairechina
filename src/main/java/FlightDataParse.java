/**
 * Created by qiang on 17-10-3.
 */


import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;



import org.apache.commons.codec.BinaryDecoder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * the purpose of this class is that to parse the
 * csv row to one entity class.
 *
 * Author: Qiang
 *
 * Version: 2017-10-03
 */
public class FlightDataParse {


    public static FlightData parse(String value) {
        String[] items = value.split(",");

        FlightData flightData = new FlightData();
        try {

            //column #0 year
            flightData.setYear(items[0]);

            //column #1 month
            flightData.setMonth(items[1]);

            //column #2 day
            flightData.setDay(items[2]);

            // column #9th flight number
            flightData.setFlightNumber(items[9]);

            // column #10th tailnumber
            flightData.setTailNumber(items[10]);

            //column arrive delay

            if(!items[14].startsWith("NA"))
                flightData.setArrDelay(Integer.parseInt(items[14]));
            else
                flightData.setArrDelay(0);

            //departure delpy
            if(!items[15].startsWith("NA"))
            flightData.setDepDelay(Integer.parseInt(items[15]));

            flightData.setOrigin(items[16]);

            flightData.setDest(items[17]);
            if(items[18].startsWith("NA"))
            flightData.setDistance(items[18]);

        }catch (Exception ex){
            ex.printStackTrace();
        }

        return flightData;

    }



}

