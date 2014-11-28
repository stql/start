package hive;
import java.io.IOException;

import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.tools.LineageInfo;

public class Test {
  public static void main(String[] args) throws IOException, ParseException,  SemanticException {

//String query = args[0];
//String query = "select a.* from a join (select * from b where id like '%哈哈%') c on a.id = c.id";

String query = "insert overwrite table youni_contact_name_his_temp2 " +
  "select a.*,b.type,b.kind1,b.kind3,b.keywords,b.match_type,b.male_rt,b.male_accuracyrate ,b.income " +
  "from youni_contact_name_his_temp a join (select type,kind1,kind3,keywords,match_type,male_rt,male_accuracyrate ,income " +
  "from youni_contact_name_type_dim where match_type='{data_desc}')b on 1=1 " +
  "where instr(a.user_name,b.keywords)>0";
LineageInfo lep = new LineageInfo();

lep.getLineageInfo(query);

for (String tab : lep.getInputTableList()) {
  System.out.println("InputTable=" + tab);
}

for (String tab : lep.getOutputTableList()) {
  System.out.println("OutputTable=" + tab);
}
}

}
