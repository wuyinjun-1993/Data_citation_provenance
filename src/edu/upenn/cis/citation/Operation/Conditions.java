package edu.upenn.cis.citation.Operation;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import edu.upenn.cis.citation.Corecover.Argument;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.Corecover.Subgoal;
import edu.upenn.cis.citation.Corecover.Tuple;
import edu.upenn.cis.citation.citation_view1.Head_strs;
import edu.upenn.cis.citation.init.init;

public class Conditions {
	
	
	public Vector<String> subgoal1 = new Vector<String>();
	
	public String agg_function1 = null;
	
	public String agg_function2 = null;
		
	public Vector<Argument> arg1 = new Vector<Argument>();
	
	public Vector<Argument> arg2 = new Vector<Argument>();
		
	public Vector<String> subgoal2 = new Vector<String>();
	
	public Operation op;
	
	public String citation_view;
	
	public String expression;
	
	public boolean get_mapping1 = true;
	
	public boolean get_mapping2 = true;
	
	public String unique_string = new String();
	
	public int id1 = -1;
	
	public int id2 = -1;
	
	static final String[] ops = {"<>",">=","<=",">","<","="};
	
	static String [] numberic_type = {"real", "bigint", "integer", "smallint", "smallint", "double precision"};
	
	public static void main(String [] args)
	{
		String str1 = "other_protein";
		
		String str2 = "gpcr";
		
		System.out.println(str1.compareTo(str2));
	}
	
	static String cal_reverse_condition_string(Conditions condition)
	{
	  String string = new String();
	  
	  if(condition.agg_function1 != null)
      {
        string += condition.agg_function1 + "(" + condition.arg1 + ")";
      }
      else
      {
        string += condition.arg1;
      }
      
      string += condition.op.counter_direction();
      
      if(condition.agg_function2 != null)
      {
        string += condition.agg_function2 + "(" + condition.arg2 + ")";
      }
      else
      {
        string += condition.arg2;
      }
      
      return string;
	  
	}
	
	@Override
	public String toString()
	{
		
	  String string = new String();
	  
	  if(agg_function1 != null)
	  {
	    string += agg_function1 + "(";
	    
	    for(int i = 0; i<arg1.size(); i++)
	    {
	      if(i >= 1)
	        string += "#";
	      
	      string += arg1.get(i);
	    }
	    
	    string += ")";
	  }
	  else
	  {
	    string += arg1.get(0);
	  }
	  
	  string += op;
	  
	  if(agg_function2 != null)
      {
        string += agg_function2 + "(";
        
        for(int i = 0; i<arg2.size(); i++)
        {
          if(i >= 1)
            string += "#";
          
          string += arg2.get(i);
        }
        
        string += ")";
      }
      else
      {
        string += arg2.get(0);
      }
	  
	  return string;
	  
//		String str = arg1.name;
//		
//		if(arg2.isConst())
//			return str + op + arg2;
//		else
//			return str + op + arg2;
	}
	
//	public String toStringinsql()
//	{
//		if(arg2.isConst())
//		{
//			String str = arg2.name.replaceAll("'", "''");
//			
//			
//			return subgoal1 + init.separator + arg1.name + op + str;
//		}
//		else
//			return subgoal1 + init.separator + arg1.name + op + subgoal2 + init.separator + arg2;
//	}
	
	static Vector<String> sort_vector_strings(Vector<Argument> args)
	{
	  Vector<String> arg_name_strings1 = new Vector<String>();
      
      for(Argument arg: args)
      {
        arg_name_strings1.add(arg.toString());
      }
      
      Collections.sort(arg_name_strings1);
      
      return arg_name_strings1;
	}
	
	static String cal_unique_string_one_side(Conditions condition)
	{
	  Vector<String> arg_string1 = sort_vector_strings(condition.arg1);
	  
	  Vector<String> arg_string2 = sort_vector_strings(condition.arg2);
	  
	  String string = new String();
	  
	  if(condition.agg_function1 != null)
	  {
	    string += condition.agg_function1;
	  }
	  
	  string += arg_string1.toString() + condition.op.toString();
	  
	  if(condition.agg_function2 != null)
	  {
	    string += condition.agg_function2;
	  }
	  
	  string += arg_string2.toString() + condition.op.toString();
	  
	  return string;
	}
	
	public String cal_unique_string()
	{
	  String condition_string1 = cal_unique_string_one_side(this);
	  
	  swap_args();
	  
	  String condition_string2 = cal_unique_string_one_side(this);
	  
	  swap_args();
	  
	  if(condition_string1.compareTo(condition_string2) >= 0)
	  {
	    return condition_string1 + "#" + condition_string2;
	  }
	  else
	  {
	    return condition_string2 + "#" + condition_string1;
	  }
      
//      if(rev_condition_string.compareTo(this.toString()) >= 0)
//      {
//        String string = rev_condition_string + "|" + this.toString();
//        
//        return string;
//      }
//      else
//      {
//        String string = this.toString() + "|" + rev_condition_string;
//        
//        return string;
//      }
	}
	
	public Conditions(Vector<Argument> arg1, Vector<String> subgoal1, Operation op, Vector<Argument> arg2, Vector<String> subgoal2, String agg_function1, String agg_function2)
	{
		this.arg1.addAll(arg1);
		
		this.op = op;
		
		this.arg2.addAll(arg2);
		
		this.subgoal1.addAll(subgoal1);
		
		this.subgoal2.addAll(subgoal2);
		
		this.agg_function1 = agg_function1;
		
		this.agg_function2 = agg_function2;
		
		this.unique_string = cal_unique_string();
	}
	
public Conditions(Argument argument, String string, Operation op2, Argument argument2,
      String string2, String agg_function12, String agg_function22) {
  
    this.arg1.add(argument);
    
    this.arg2.add(argument2);
    
    this.subgoal1.add(string);
    
    this.subgoal2.add(string2);
    
    this.op = op2;
    
    this.agg_function1 = agg_function12;
    
    this.agg_function2 = agg_function22;
    
    this.unique_string = cal_unique_string();
    // TODO Auto-generated constructor stub
  }


  public Conditions(Argument argument, String string, Operation op2, Argument argument2,
      String string2) {
  
    this.arg1.add(argument);
    
    this.arg2.add(argument2);
    
    this.subgoal1.add(string);
    
    this.subgoal2.add(string2);
    
    this.op = op2;
    
//    this.agg_function1 = agg_function12;
//    
//    this.agg_function2 = agg_function22;
    
    this.unique_string = cal_unique_string();
    // TODO Auto-generated constructor stub
  }

  //	public static Conditions parse(String constraint, Vector<Subgoal> subgoals, HashMap<String, String> origin_names)
//	{
//		int i = 0;
//		
//		for(i = 0; i< ops.length; i++)
//		{
//			if(constraint.contains(ops[i]))
//				break;
//		}
//		
//		String [] items = constraint.split(ops[i]);
//		
//		Argument arg1 = new Argument(items[0].trim(), origin_names.get(items[0].trim()));
//		
//		Argument arg2 = new Argument(items[1].trim(), origin_names.get(items[1].trim()));
//		
//		String sg1 = new String();
//		
//		String sg2 = new String();
//		
//		for(int j = 0; j<subgoals.size(); j++)
//		{
//			if(subgoals.get(j).args.contains(arg1))
//			{
//				sg1 = subgoals.get(j).name;
//				break;
//			}
//		}
//		
//		if(arg2.type != 1)
//		for(int j = 0; j<subgoals.size(); j++)
//		{	
//			if(subgoals.get(j).args.contains(arg2))
//			{
//				sg2 = subgoals.get(j).name;
//				break;
//			}
//			
//		}
//		
//		
//		Operation op;
//		
//		switch(i)
//		{
//		case 0: op = new op_not_equal(); break;
//		case 1: op = new op_greater_equal(); break;
//		case 2: op = new op_less_equal(); break;
//		case 3: op = new op_greater(); break;
//		case 4: op = new op_less(); break;
//		case 5: op = new op_equal(); break;
//		default: op = null; break;
//		}
//		
//		return new Conditions(arg1, sg1, op, arg2, sg2);
//		
//	}
//	
	public static Conditions negation(Conditions conditions)
	{
		
		Conditions condition = new Conditions(conditions.arg1, conditions.subgoal1, conditions.op.negation(), conditions.arg2, conditions.subgoal2, conditions.agg_function1, conditions.agg_function2);
				
		return conditions;
	}
	
	
	public void negation()
	{
		op = op.negation();
	}
	
	public static boolean compare(Conditions c1, Conditions c2)
	{
//		if((c1.arg1.origin_name.equals(c2.arg1.origin_name) && c1.arg2.origin_name.equals(c2.arg2.origin_name) && c1.subgoal1.equals(c2.subgoal1) && c1.subgoal2.equals(c2.subgoal2) ))
		
	  if(c1.unique_string.equals(c2.unique_string))
	    return true;
	  
	  return false;
	  
//		if(c1.toString().equals(c2.toString()))
//			 return true;
//		
//		if(c1.toString().equals(reverse_condition(c2).toString()))
//			return true;
//		
//		if(c1.arg2.isConst() && c2.arg2.isConst() && (c1.subgoal1.toString() + init.separator + c1.arg1.name).equals(c2.subgoal1.toString() + init.separator + c2.arg1.name))
//		{
//			try{
//				double c1_arg2 = Double.valueOf(c1.arg2.name);
//				
//				double c2_arg2 = Double.valueOf(c2.arg2.name);
//				
//				if(c1.op.equals(c2.op))
//				{
//					if((c1.op.toString().equals(">") || c1.op.toString().equals(">=")) && (c1_arg2 >= c2_arg2))
//						return true;
//					
//					if((c1.op.toString().equals("<") || c1.op.toString().equals("<=")) && (c1_arg2 <= c2_arg2))
//						return true;
//				}
//				
//				if(c1.op.toString().equals(">") && c2.op.toString().equals(">=") && c1_arg2 >= c2_arg2)
//					return true;
//				
//				if(c1.op.toString().equals("<") && c2.op.toString().equals("<=") && c1_arg2 <= c2_arg2)
//					return true;
//				
//				if(c1.op.toString().equals(">=") && c2.op.toString().equals(">") && c1_arg2 > c2_arg2)
//					return true;
//				
//				if(c1.op.toString().equals("<=") && c2.op.toString().equals("<") && c1_arg2 < c2_arg2)
//					return true;
//				
//				
//			}
//			catch(Exception e)
//			{
//				String c1_arg2 = c1.arg2.name;
//				
//				String c2_arg2 = c2.arg2.name;
//				
//				if(c1.op.equals(c2.op))
//				{
//					if((c1.op.toString().equals(">") || c1.op.toString().equals(">=")) && (c1_arg2.compareTo(c2_arg2)) >= 0)
//						return true;
//					
//					if((c1.op.toString().equals("<") || c1.op.toString().equals("<=")) && (c1_arg2.compareTo(c2_arg2)) <= 0)
//						return true;
//				}
//				
//				if(c1.op.toString().equals(">") && c2.op.toString().equals(">=") && (c1_arg2.compareTo(c2_arg2)) >= 0)
//					return true;
//				
//				if(c1.op.toString().equals("<") && c2.op.toString().equals("<=") && (c1_arg2.compareTo(c2_arg2)) <= 0)
//					return true;
//				
//				if(c1.op.toString().equals(">=") && c2.op.toString().equals(">") && (c1_arg2.compareTo(c2_arg2)) > 0)
//					return true;
//				
//				if(c1.op.toString().equals("<=") && c2.op.toString().equals("<") && (c1_arg2.compareTo(c2_arg2)) < 0)
//					return true;
//			}
//			
//			
//		}
//		
//		
//		return false;
//		
//		if((c1.arg2.origin_name.equals(c2.arg1.origin_name) && c1.arg1.origin_name.equals(c2.arg2.origin_name) && c1.subgoal2.equals(c2.subgoal1) && c1.subgoal1.equals(c2.subgoal2) ))
    }
	
	static Conditions reverse_condition(Conditions c)
	{
		Operation op = c.op.negation();
		
		return new Conditions(c.arg2, c.subgoal2, op, c.arg1, c.subgoal1, c.agg_function2, c.agg_function1);
	}

	
//	public static Conditions parse(String condition_str)
//	{
//		int i = 0;
//		
//		for(i = 0; i< ops.length; i++)
//		{
//			if(condition_str.contains(ops[i]))
//				break;
//		}
//		
//		
//		Operation op;
//		
//		switch(i)
//		{
//		case 0: op = new op_not_equal(); break;
//		case 1: op = new op_greater_equal(); break;
//		case 2: op = new op_less_equal(); break;
//		case 3: op = new op_greater(); break;
//		case 4: op = new op_less(); break;
//		case 5: op = new op_equal(); break;
//		default: op = null; break;
//		}
//		
//		String [] args = condition_str.split(op.toString());
//		
//		String arg1_str = args[0].trim();
//		
//		String []strs = arg1_str.split("_");
//		
//		String t1 = strs[0] + "_" + strs[1];
//		
//		String arg1 = arg1_str;//.substring(t1.length() + 1, arg1_str.length());
//		
//		if(!args[1].contains("'"))
//		{
//			String arg2_str = args[1].trim();
//			
//			strs = arg2_str.split("_");
//			
//			String t2 = strs[0] + "_" + strs[1];
//			
//			String arg2 = arg2_str;//.substring(t2.length() + 1, arg2_str.length());
//			
//			return new Conditions(new Argument(arg1, arg1), t1, op, new Argument(arg2, arg2), t2);
//
//		}
//		
//		else
//		{
//			return new Conditions(new Argument(arg1, arg1), t1, op, new Argument(args[1], arg1), new String());
//		}
//		
//		
//		
//		
//	}
//	
	
	@Override
	public boolean equals(Object obj)
	{
		
		Conditions condition = (Conditions) obj;
		
//		if(this.arg1.name.equals(condition.arg1.name) && this.arg1.relation_name.equals(condition.arg1.relation_name) 
//				&& this.arg2.name.equals(condition.arg2.name) && this.arg2.relation_name.equals(condition.arg2.relation_name) 
//				&& this.subgoal1.equals(condition.subgoal1) && this.subgoal2.equals(condition.subgoal2) && this.op.get_op_name().equals(condition.op.get_op_name()))
//			return true;
		
		return this.hashCode() == condition.hashCode();
	}
	
	@Override
	public int hashCode()
	{
//		return this.arg1.hashCode() * 10000 + this.subgoal1.hashCode()*1000 + this.op.hashCode()*100 + this.arg2.hashCode()*10 + this.subgoal2.hashCode();
	  
	  return unique_string.hashCode();
	  
	  
	}
	
	
	static boolean check_operator_match(Operation op1, Operation op2)
	{
	  String op_name1 = op1.get_op_name();
	  
	  String op_name2 = op2.get_op_name();
	  
	  
	  if(op_name1.equals("="))
	  {
	    return op_name2.equals("=");
	  }
	  else
	  {
	    if(op_name1.equals(">"))
	    {
	      return op_name2.equals(">");
	    }
	    else
	    {
	      if(op_name1.equals(">="))
	      {
	        return op_name2.equals(">=") || op_name2.equals(">");
	      }
	      else
	      {
	        if(op_name1.equals("<"))
	          return op_name2.equals("<");
	        else
	        {
	          return op_name2.equals("<=") || op_name2.equals("<");
	        }
	      }
	    }
	  }
	}
	
	
	static boolean check_local_predicate_match(Argument arg1, Argument arg2, Operation op1, Operation op2, Tuple tuple)
	{
	  HashSet<String> types = new HashSet<String>(Arrays.asList(numberic_type));
      
	  String type1 = arg1.data_type;
	  
	  String string1 = arg1.name;
	  
	  String string2 = arg2.name;
	  
      if(types.contains(type1))
      {
          double value1 = Double.valueOf(string1);
          
          double value2 = Double.valueOf(string1);
          
          if(value1 < value2)
          {
              return check_smaller_greater_values(op1, op2);
          }
          else
          {
              if(value1 > value2)
              {
                  return check_greater_smaller_values(op1, op2);
              }
              else
              {
                  if(op1.equals(op2))
                      return true;
                  else
                      return false;
              }
          }
      }
      else
      {
          if(string1.compareToIgnoreCase(string2) < 0)
          {
              
              return check_smaller_greater_values(op1, op2);
              
          }
          else
          {
              if(string1.compareToIgnoreCase(string2) > 0)
              {
                  return check_greater_smaller_values(op1, op2);
              }
              else
              {
                  if(op1.equals(op2))
                      return true;
                  else
                      return false;
              }
          }
      }
	}
	
	
	static boolean check_predicate_match_without_agg(Conditions condition1, Conditions condition2, Tuple tuple)
	{
	  if(condition1.arg2.size() == 1 && condition1.arg2.get(0).isConst())
      {
          if(condition2.arg2.get(0).isConst() && tuple.phi.apply(condition1.arg1.get(0)).equals(condition2.arg1.get(0)) && check_operator_match(condition1.op, condition2.op))
          {
              return check_local_predicate_match(condition1.arg2.get(0), condition2.arg2.get(0), condition1.op, condition2.op, tuple);
          }
      }
      else
      {
          if(!condition2.arg2.get(0).isConst() && tuple.phi.apply(condition1.arg1.get(0)).equals(condition2.arg1.get(0))
              && tuple.phi.apply(condition1.arg2.get(0)).equals(condition2.arg2.get(0)) && check_operator_match(condition1.op, condition2.op))
          {
              return true;
          }
          else
          {
            if(!condition2.arg2.get(0).isConst() && tuple.phi.apply(condition1.arg2.get(0)).equals(condition2.arg1.get(0))
                && tuple.phi.apply(condition1.arg1.get(0)).equals(condition2.arg2.get(0)) && check_operator_match(condition1.op.counter_direction(), condition2.op))
            {
                return true;
            }
          }
          
      }
      
      return false;
	}
	
	public static boolean check_predicate_match(Conditions condition1, Conditions condition2, Tuple tuple)
	{
	  
	  if(condition1.agg_function1 == null && condition1.agg_function2 == null)
	  {
	    if(condition2.agg_function1 == null && condition2.agg_function2 == null)
	    {
	      return check_predicate_match_without_agg(condition1, condition2, tuple);
	    }
	    else
	      return false;
	  }
	  else
	  {
	    if(condition1.agg_function1 != null && condition1.agg_function2 != null)
	    {
	      if(condition2.agg_function1 != null && condition2.agg_function2 != null)
	      {
	        return check_predicate_match_with_agg(condition1, condition2, tuple);
	      }
	    }
	    else
	    {
	      if(condition1.agg_function1 != null)
	      {
	          if(condition2.agg_function1 != null)
	          {
	              return check_predicate_match_with_agg(condition1, condition2, tuple);
	          }
	          else
	          {
	            if(condition2.agg_function2 != null)
	            {
	                  return check_predicate_match_with_agg(condition1, condition2, tuple);
	            }
	          }
	      }
	    }
	  }
	  
	  return false;
	}
	
	
	static boolean check_agg_arg_match(Vector<Argument> args1, Vector<Argument> args2, Tuple tuple)
	{
	  if(args1.size() != args2.size())
	    return false;
	  for(int i = 0; i<args1.size(); i++)
	  {
	    Argument arg1 = args1.get(i);
	    
	    if(!args2.contains(tuple.phi.apply(arg1)))
	    {
	      return false;
	    }
	  }
	  
	  for(int i = 0; i<args2.size(); i++)
      {
        Argument arg2 = args2.get(i);
        
        if(!args1.contains(tuple.reverse_phi.apply(arg2)))
        {
          return false;
        }
      }
	  
	  return true;
	}
	
	static boolean check_predicate_match_with_agg(Conditions condition1, Conditions condition2, Tuple tuple)
	{
	  if(condition1.arg2.size() == 1 && condition1.arg2.get(0).isConst())
      {
          if(condition2.arg2.size() == 1 && condition2.arg2.get(0).isConst() && check_operator_match(condition1.op, condition2.op) && condition1.agg_function1.equals(condition2.agg_function1))
          {
             return check_agg_arg_match(condition1.arg1, condition2.arg1, tuple);
          }
      }
	  else
	  {
	    if(check_operator_match(condition1.op, condition2.op) && condition1.agg_function1.equals(condition2.agg_function1) && condition1.agg_function2.equals(condition2.agg_function2))
	    {
	      return check_agg_arg_match(condition1.arg1, condition2.arg1, tuple) && check_agg_arg_match(condition1.arg2, condition2.arg2, tuple);
	    }
	    else
	    {
	      if(check_operator_match(condition1.op.counter_direction(), condition2.op) && condition1.agg_function2.equals(condition2.agg_function1) && condition1.agg_function1.equals(condition2.agg_function2))
	      {
	        return check_agg_arg_match(condition1.arg2, condition2.arg1, tuple) && check_agg_arg_match(condition1.arg1, condition2.arg2, tuple);
	      }
	    }
	    
	  }
	  
	  return false;
	}
	
	static boolean check_smaller_greater_values(Operation op1, Operation op2)
	{
		if(op1.toString().equals(">") || op1.toString().equals(">="))
		{
			if(op2.toString().equals(">") || op2.toString().equals(">=") || op2.toString().equals("="))
			{
				return true;
			}
		}
		
		return false;
	}
	
	static boolean check_greater_smaller_values(Operation op1, Operation op2)
	{
		if(op1.toString().equals("<") || op1.toString().equals("<="))
		{
			if(op2.toString().equals("<") || op2.toString().equals("<=") || op2.toString().equals("="))
			{
				return true;
			}
		}
		
		return false;
	}
	
	//condition1: view condition;;condition2:query condition
	public static boolean check_predicates_satisfy(Conditions condition1, Conditions condition2, Query query)
	{
		if(condition1.arg2.size() == 1 && condition1.arg2.get(0).isConst())
		{
			String string1 = condition1.arg2.get(0).name;
			
			String string2 = condition2.arg2.get(0).name;
			
			String relation = condition1.subgoal1.get(0);
			
			String type1 = condition1.arg1.get(0).data_type;
			
			Operation op1 = condition1.op;
			
			Operation op2 = condition2.op;
			
			String arg_name = condition1.arg1.get(0).attribute_name;//.name.substring(condition1.arg1.get(0).name.indexOf(init.separator) + 1, condition1.arg1.get(0).name.length());
			
			HashSet<String> types = new HashSet<String>(Arrays.asList(numberic_type));
			
			if(types.contains(type1))
			{
				double value1 = Double.valueOf(string1);
				
				double value2 = Double.valueOf(string1);
				
				if(value1 < value2)
				{
					return check_smaller_greater_values(op1, op2);
				}
				else
				{
					if(value1 > value2)
					{
						return check_greater_smaller_values(op1, op2);
					}
					else
					{
						if(op1.equals(op2))
							return true;
						else
							return false;
					}
				}
			}
			else
			{
				if(string1.compareToIgnoreCase(string2) < 0)
				{
					
					return check_smaller_greater_values(op1, op2);
					
				}
				else
				{
					if(string1.compareToIgnoreCase(string2) > 0)
					{
						return check_greater_smaller_values(op1, op2);
					}
					else
					{
						if(op1.equals(op2))
							return true;
						else
							return false;
					}
				}
			}
		}
		else
		{
			Operation op1 = condition1.op;
			
			Operation op2 = condition2.op;
			
			if(op1.equals(op2))
				return true;
			else
			{
				if(op1.toString().equals(">="))
				{
					if(op2.toString().equals(">") || op2.toString().equals("="))
						return true;
				}
				else
				{
					if(op1.toString().equals("<="))
					{
						if(op2.toString().equals("<") || op2.toString().equals("="))
							return true;
					}
				}
			}
			
			return false;
		}
	}
	
	static void swap_vectors(Vector v1, Vector v2)
	{
	  Vector temp = new Vector();
	  
	  temp.addAll(v1);
	  
	  v1.clear();
	  
	  v1.addAll(v2);
	  
	  v2.clear();
	  
	  v2.addAll(temp);
	}
	
	public void swap_args()
	{
	  swap_vectors(arg1, arg2);

	  swap_vectors(subgoal1, subgoal2);
	  
	  op = op.counter_direction();

	  boolean b_temp = get_mapping1;
	  
	  get_mapping1 = get_mapping2;
	  
	  get_mapping2 = b_temp;
	  
	  String agg_function_temp = agg_function1;
	  
	  agg_function1 = agg_function2;
	  
	  agg_function2 = agg_function_temp;
	  
	}
	
}
