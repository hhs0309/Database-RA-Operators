
/****************************************************************************************
 * @file  Table.java
 *
 * @author   John Miller
 */

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import static java.lang.Boolean.*;
import static java.lang.System.out;

/****************************************************************************************
 * This class implements relational database tables (including attribute names, domains
 * and a list of tuples.  Five basic relational algebra operators are provided: project,
 * select, union, minus and join.  The insert data manipulation operator is also provided.
 * Missing are update and delete data manipulation operators.
 */
public class Table
       implements Serializable
{
    /** Relative path for storage directory
     */
    private static final String DIR = "store" + File.separator;

    /** Filename extension for database files
     */
    private static final String EXT = ".dbf";

    /** Counter for naming temporary tables.
     */
    private static int count = 0;

    /** Table name.
     */
    private final String name;

    /** Array of attribute names.
     */
    private final String [] attribute;

    /** Array of attribute domains: a domain may be
     *  integer types: Long, Integer, Short, Byte
     *  real types: Double, Float
     *  string types: Character, String
     */
    private final Class [] domain;

    /** Collection of tuples (data storage).
     */
    private final List <Comparable []> tuples;

    /** Primary key. 
     */
    private final String [] key;

    /** Index into tuples (maps key to tuple number).
     */
    private final Map <KeyType, Comparable []> index;

    /** The supported map types.
     */
    private enum MapType { NO_MAP, TREE_MAP, LINHASH_MAP, BPTREE_MAP }

    /** The map type to be used for indices.  Change as needed.
     */
    private static final MapType mType = MapType.NO_MAP;

    /************************************************************************************
     * Make a map (index) given the MapType.
     */
    private static Map <KeyType, Comparable []> makeMap ()
    {
        switch (mType) {
        case TREE_MAP:    return new TreeMap <> ();
        //case LINHASH_MAP: return new LinHashMap <> (KeyType.class, Comparable [].class);
        //case BPTREE_MAP:  return new BpTreeMap <> (KeyType.class, Comparable [].class);
        default:          return null;
        } // switch
    } // makeMap

    //-----------------------------------------------------------------------------------
    // Constructors
    //-----------------------------------------------------------------------------------

    /************************************************************************************
     * Construct an empty table from the meta-data specifications.
     *
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     */  
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = new ArrayList <> ();
        index     = makeMap ();

    } // primary constructor

    /************************************************************************************
     * Construct a table from the meta-data specifications and data in _tuples list.
     *
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     * @param _tuples     the list of tuples containing the data
     */  
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key,
                  List <Comparable []> _tuples)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = _tuples;
        index     = makeMap ();
    } // constructor

    /************************************************************************************
     * Construct an empty table from the raw string specifications.
     *
     * @param _name       the name of the relation
     * @param attributes  the string containing attributes names
     * @param domains     the string containing attribute domains (data types)
     * @param _key        the primary key
     */
    public Table (String _name, String attributes, String domains, String _key)
    {
        this (_name, attributes.split (" "), findClass (domains.split (" ")), _key.split(" "));

        out.println ("DDL> create table " + name + " (" + attributes + ")");
    } // constructor

    //----------------------------------------------------------------------------------
    // Public Methods
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Project the tuples onto a lower dimension by keeping only the given attributes.
     * Check whether the original key is included in the projection.
     *
     * #usage movie.project ("title year studioNo")
     *
     * @param attributes  the attributes to project onto
     * @return  a table of projected tuples
     * @author Gibson Foss
     */
    public Table project (String attributes)
    {
		out.println("RA> " + name + ".project (" + attributes + ")");
		var attrs = attributes.split(" ");
		List<String> finalAttributes = new ArrayList<>();
		var orinalArrtibutes = attribute.toString().split(" ");
		Set<String> set = new TreeSet<>();
		for (int i = 0; i < attribute.length; i++)
			set.add(attribute[i]);

		for (int i = 0; i < attrs.length; i++) {
			if (!set.contains(attrs[i])) {
				System.err.println("No domain found for :" + attrs[i]);
			} else {
				finalAttributes.add(attrs[i]);
			}
		}

		String[] stringArray = finalAttributes.toArray(new String[0]);
		var colDomain = extractDom(match(stringArray), domain);
		var newKey = (Arrays.asList(stringArray).containsAll(Arrays.asList(key))) ? key : attrs;

		List<Comparable[]> rows = new ArrayList<>();
		// Take a stream of tuples for the given table and then extract that attributes
		// and that particular tuple using -
		// pre-made extraction method provided by Dr. Miller.

		this.tuples.stream().forEach(element -> rows.add(extract(element, stringArray)));

		return new Table(name + count++, stringArray, colDomain, newKey, rows);
    } // project

    /************************************************************************************
     * Select the tuples satisfying the given predicate (Boolean function).
     *
     * #usage movie.select (t -> t[movie.col("year")].equals (1977))
     *
     * @param predicate  the check condition for tuples
     * @return  a table with tuples satisfying the predicate
     */
    public Table select (Predicate <Comparable []> predicate)
    {
        out.println ("RA> " + name + ".select (" + predicate + ")");

        return new Table (name + count++, attribute, domain, key,
                   tuples.stream ().filter (t -> predicate.test (t))
                                   .collect (Collectors.toList ()));
    } // select

    /************************************************************************************
     * Select the tuples satisfying the given key predicate (key = value).  Use an index
     * (Map) to retrieve the tuple with the given key value.
     *
     * @param keyVal  the given key value
     * @return  a table with the tuple satisfying the key predicate
     * @author Radhika Bhavsar 
     */
	public Table select(KeyType keyVal) {

		List<Comparable[]> rows = new ArrayList<>();
		try {
			out.println("RA> " + name + ".select (" + keyVal + ")");
			// if Index is used, simply retrieve the value based on the input key
			if (mType != MapType.NO_MAP) {
				if (index.get(keyVal) != null)
					rows.add(index.get(keyVal));
			}
			// if No Index has been specified, iterate through each tuple and add those
			// tuples to the rows that satisfy the input key
			else {

				tuples.forEach(t -> {
					for (int i = 0; i < attribute.length; i++) {
						if (new KeyType(t[i].toString()).equals(keyVal)) {
							rows.add(t);
						}
					}
				});
			}

		} catch (Exception e) {
			System.err.println("Something went wrong. Please try again. Key might be null ");
		}
		return new Table(name + count++, attribute, domain, key, rows);

	} // select
    /************************************************************************************
     * Union this table and table2.  Check that the two tables are compatible.
     *
     * #usage movie.union (show)
     *
     * @param table2  the rhs table in the union operation
     * @return  a table representing the union
     * @author Niyati Shah
     */
	public Table union (Table table2)
    {
        out.println ("RA> " + name + ".union (" + table2.name + ")");
        if (! compatible (table2)) return null;

        List <Comparable []> rows = new ArrayList <> ();
        //union with duplicate tuples
       // Stream.concat(this.tuples.stream(), table2.tuples.stream()).collect(Collectors.toList()).stream().forEach(item -> rows.add(item));
        //union with no duplicate tuples
        //adding tuples of the 1st table in the new table using for each loop 
        this.tuples.stream().forEach(items -> rows.add(items));
        //adding tuples from the second table and checking that the tuple in the second table doesnt match the tuple in the first table, if it matches filtering out those tuple
        table2.tuples.stream().filter(item -> !(this.tuples.contains(item))) .forEach(item -> rows.add(item));
        return new Table (name + count++, attribute, domain, key, rows);
        
    } // union

    /************************************************************************************
     * Take the difference of this table and table2.  Check that the two tables are
     * compatible.
     *
     * #usage movie.minus (show)
     *
     * @param table2  The rhs table in the minus operation
     * @return  a table representing the difference
     * @author Tusharika Mishra
     */
    public Table minus (Table table2)
    {
        out.println ("RA> " + name + ".minus (" + table2.name + ")");
        List <Comparable []> rows = new ArrayList <> ();
        //1. If table types are not similar , it results to NullPointer Exception
       // if (! compatible (table2)) return null;
        //2. check if they are union compatible
        //3. use foreach to add tuples of table 1 which are not present in table 2
        //4. filter tuples from table1 and table2
        // R1 - (R1 interesection R2)    
         if (!compatible (table2)) return null;
        this.tuples.stream().filter(item -> !(table2.tuples.contains(item)))
                         .forEach(item -> rows.add(item));
        return new Table (name + count++, attribute, domain, key, rows);
    } // minus

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Tuples from both tables
     * are compared requiring attributes1 to equal attributes2.  Disambiguate attribute
     * names by append "2" to the end of any duplicate attribute name.  Implement using
     * a Nested Loop Join algorithm.
     *
     * #usage movie.join ("studioNo", "name", studio)
     *
     * @param attribute1  the attributes of this table to be compared (Foreign Key)
     * @param attribute2  the attributes of table2 to be compared (Primary Key)
     * @param table2      the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     * @author Hiten Nirmal
     * @author Niyati Shah
     */
    public Table join (String attributes1, String attributes2, Table table2)
    {
        out.println ("RA> " + name + ".join (" + attributes1 + ", " + attributes2 + ", "
                                               + table2.name + ")");
        String[] t_attrs = attributes1.split (" ");
        String[] u_attrs = attributes2.split (" "); 
        var rows    = new ArrayList <Comparable []> ();
        //take tuples from both the table and find common elements
        // if common element is found then add that tuple to the table
        // common elements are found by calling a method
        this.tuples.stream().forEach(items->
        {
        	table2.tuples.stream().forEach(i->
        	{
        		if(checkCommonElements(items,i,t_attrs,u_attrs, table2))
        		{       			
        			rows.add(ArrayUtil.concat(items,i));		
        		}
        	});
        });      
        //Disambiguate attribute names by append "1" to the end of any duplicate attribute name.
        for(int i=0;i<attribute.length;i++)
        {
        	for(int j=0;j<table2.attribute.length;j++)
        	{
        		if (attribute[i].equals(table2.attribute[j]))
        		{
        			attribute[i]=attribute[i]+"1";
        		}
        	}
      }
        return new Table (name + count++, ArrayUtil.concat (attribute, table2.attribute),
                                          ArrayUtil.concat (domain, table2.domain), key, rows);
       } // join

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Same as above, but implemented
     * using an Index Join algorithm.
     *
     * @param attribute1  the attributes of this table to be compared (Foreign Key)
     * @param attribute2  the attributes of table2 to be compared (Primary Key)
     * @param table2      the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table i_join (String attributes1, String attributes2, Table table2)
    {
        return null;
    } // i_join

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Same as above, but implemented
     * using a Hash Join algorithm.
     *
     * @param attribute1  the attributes of this table to be compared (Foreign Key)
     * @param attribute2  the attributes of table2 to be compared (Primary Key)
     * @param table2      the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table h_join (String attributes1, String attributes2, Table table2)
    {
        return null;
    } // h_join

    /************************************************************************************
     * Join this table and table2 by performing an "natural join".  Tuples from both tables
     * are compared requiring common attributes to be equal.  The duplicate column is also
     * eliminated.
     *
     * #usage movieStar.join (starsIn)
     *
     * @param table2  the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     * @author Hiten Nirmal 
     * @author Niyati Shah
     */
    public Table join (Table table2)
    {
       out.println ("RA> " + name + ".join (" + table2.name + ")");
        var rows = new ArrayList <Comparable []> ();
        List<String> newTable2Attrs = new ArrayList<String>();
        List<String> matchingColumns = new ArrayList<String>();
       // Checking if the two tables have any attributes in common and putting the matching attributes in one list and rest attributes in other list
        //newTable2Attrs contain unmatched attributes and matchingColumns have matching attributes
        for (String attr_t2: table2.attribute) {
        	if (!Arrays.asList(this.attribute).contains(attr_t2)) {
        		newTable2Attrs.add(attr_t2);
        	}else {
        		matchingColumns.add(attr_t2);
        	}
        }
        System.out.println("matching attributes********** "+ matchingColumns);
        System.out.println("unmatching attributes********** "+ newTable2Attrs);
        //for each tuple in table1: it is checking all the tuples in table2 and if the matching attributes have same element in both table, 
        //this is done by calling the method checkCommonElements which returns the boolean value.
        //then it extracts the tuple from table1 and and the tuple from table2 except for the uncommon attributes. 
        this.tuples.stream().forEach(items->
        {
        	table2.tuples.stream().forEach(i->
        	{
        		if(checkCommonElements(items,i,matchingColumns))
        		{	
        			rows.add(ArrayUtil.concat(items, table2.extract(i,newTable2Attrs.toArray(new String[newTable2Attrs.size()]))));			
        		}
        	});
        });      
        //done FIXing - eliminating duplicate columns
        return new Table (name + count++, ArrayUtil.concat (attribute, newTable2Attrs.toArray(new String [newTable2Attrs.size()])),
        		ArrayUtil.concat (domain, table2.domain), key, rows);
        } // join

    /************************************************************************************
     * Return the column position for the given attribute name.
     *
     * @param attr  the given attribute name
     * @return  a column position
     */
    public int col (String attr)
    {
        for (int i = 0; i < attribute.length; i++) {
           if (attr.equals (attribute [i])) return i;
        } // for

        return -1;  // not found
    } // col

    /************************************************************************************
     * Insert a tuple to the table.
     *
     * #usage movie.insert ("'Star_Wars'", 1977, 124, "T", "Fox", 12345)
     *
     * @param tup  the array of attribute values forming the tuple
     * @return  whether insertion was successful
     */
    public boolean insert (Comparable [] tup)
    {
        out.println ("DML> insert into " + name + " values ( " + Arrays.toString (tup) + " )");

        if (typeCheck (tup)) {
            tuples.add (tup);
            var keyVal = new Comparable [key.length];
            var cols   = match (key);
            for (int j = 0; j < keyVal.length; j++) keyVal [j] = tup [cols [j]];
            if (mType != MapType.NO_MAP) index.put (new KeyType (keyVal), tup);
            return true;
        } else {
            return false;
        } // if
    } // insert

    /************************************************************************************
     * Get the name of the table.
     *
     * @return  the table's name
     */
    public String getName ()
    {
        return name;
    } // getName

    /************************************************************************************
     * Print this table.
     */
    public void print ()
    {
        out.println ("\n Table " + name);
        out.print ("|-");
        for (int i = 0; i < attribute.length; i++) out.print ("---------------");
        out.println ("-|");
        out.print ("| ");
        for (String a : attribute) out.printf ("%15s", a);
        out.println (" |");
        out.print ("|-");
        for (int i = 0; i < attribute.length; i++) out.print ("---------------");
        out.println ("-|");
        for (Comparable [] tup : tuples) {
            out.print ("| ");
            for (Comparable attr : tup) out.printf ("%15s", attr);
            out.println (" |");
        } // for
        out.print ("|-");
        for (int i = 0; i < attribute.length; i++) out.print ("---------------");
        out.println ("-|");
    } // print

    /************************************************************************************
     * Print this table's index (Map).
     */
    public void printIndex ()
    {
        out.println ("\n Index for " + name);
        out.println ("-------------------");
        if (mType != MapType.NO_MAP) {
            for (Map.Entry <KeyType, Comparable []> e : index.entrySet ()) {
                out.println (e.getKey () + " -> " + Arrays.toString (e.getValue ()));
            } // for
        } // if
        out.println ("-------------------");
    } // printIndex

    /************************************************************************************
     * Load the table with the given name into memory. 
     *
     * @param name  the name of the table to load
     */
    public static Table load (String name)
    {
        Table tab = null;
        try {
            ObjectInputStream ois = new ObjectInputStream (new FileInputStream (DIR + name + EXT));
            tab = (Table) ois.readObject ();
            ois.close ();
        } catch (IOException ex) {
            out.println ("load: IO Exception");
            ex.printStackTrace ();
        } catch (ClassNotFoundException ex) {
            out.println ("load: Class Not Found Exception");
            ex.printStackTrace ();
        } // try
        return tab;
    } // load

    /************************************************************************************
     * Save this table in a file.
     */
    public void save ()
    {
        try {
            var oos = new ObjectOutputStream (new FileOutputStream (DIR + name + EXT));
            oos.writeObject (this);
            oos.close ();
        } catch (IOException ex) {
            out.println ("save: IO Exception");
            ex.printStackTrace ();
        } // try
    } // save

    //----------------------------------------------------------------------------------
    // Private Methods
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Determine whether the two tables (this and table2) are compatible, i.e., have
     * the same number of attributes each with the same corresponding domain.
     *
     * @param table2  the rhs table
     * @return  whether the two tables are compatible
     */
    private boolean compatible (Table table2)
    {
        if (domain.length != table2.domain.length) {
            out.println ("compatible ERROR: table have different arity");
            return false;
        } // if
        for (int j = 0; j < domain.length; j++) {
            if (domain [j] != table2.domain [j]) {
                out.println ("compatible ERROR: tables disagree on domain " + j);
                return false;
            } // if
        } // for
        return true;
    } // compatible

    /************************************************************************************
     * Match the column and attribute names to determine the domains.
     *
     * @param column  the array of column names
     * @return  an array of column index positions
     */
    private int [] match (String [] column)
    {
        int [] colPos = new int [column.length];

        for (int j = 0; j < column.length; j++) {
            var matched = false;
            for (int k = 0; k < attribute.length; k++) {
                if (column [j].equals (attribute [k])) {
                    matched = true;
                    colPos [j] = k;
                } // for
            } // for
            if ( ! matched) {
                out.println ("match: domain not found for " + column [j]);
            } // if
        } // for

        return colPos;
    } // match

    /************************************************************************************
     * Extract the attributes specified by the column array from tuple t.
     *
     * @param t       the tuple to extract from
     * @param column  the array of column names
     * @return  a smaller tuple extracted from tuple t 
     */
    private Comparable [] extract (Comparable [] t, String [] column)
    {
        var tup    = new Comparable [column.length];
        var colPos = match (column);
        for (int j = 0; j < column.length; j++) tup [j] = t [colPos [j]];
        return tup;
    } // extract

    /************************************************************************************
     * Check the size of the tuple (number of elements in list) as well as the type of
     * each value to ensure it is from the right domain. 
     *
     * @param t  the tuple as a list of attribute values
     * @return  whether the tuple has the right size and values that comply
     *          with the given domains
     */
    private boolean typeCheck (Comparable [] t)
    { 
        //  T O   B E   I M P L E M E N T E D 

        return true;
    } // typeCheck

    /************************************************************************************
     * Find the classes in the "java.lang" package with given names.
     *
     * @param className  the array of class name (e.g., {"Integer", "String"})
     * @return  an array of Java classes
     */
    private static Class [] findClass (String [] className)
    {
        var classArray = new Class [className.length];

        for (int i = 0; i < className.length; i++) {
            try {
                classArray [i] = Class.forName ("java.lang." + className [i]);
            } catch (ClassNotFoundException ex) {
                out.println ("findClass: " + ex);
            } // try
        } // for

        return classArray;
    } // findClass

    /************************************************************************************
     * Extract the corresponding domains.
     *
     * @param colPos the column positions to extract.
     * @param group  where to extract from
     * @return  the extracted domains
     */
    private Class [] extractDom (int [] colPos, Class [] group)
    {
        var obj = new Class [colPos.length];

        for (int j = 0; j < colPos.length; j++) {
            obj [j] = group [colPos [j]];
        } // for

        return obj;
    } // extractDom
    /************************************************************************************
     * Compare two tuples 
     *
     * @param 'a' elements of this tuple
     * @param 'b' elements of second table tuple;
     * @param 'c' matching attribute
     * @return boolean value- true or false
     * @author Hiten Nirmal
     * @author Niyati Shah
     */
    public boolean checkCommonElements(Comparable[] a, Comparable b[], List<String> c)
    {
	   //This method is used of checking the element of matching attributed in both the tables and if the attributes 
       //are same it returns true else false.
	   Comparable[] extract1;
	   extract1 = extract(a,c.toArray(new String[c.size()]));
	   Comparable[] extract2;
	   extract2 = extract(b,c.toArray(new String[c.size()]));
	   for(int i=0;i<extract1.length;i++)
	   {
		   if(extract1[i].equals(extract2[i]))
			   continue;
		   else
			  return false;
	   }
	   return true;
    }
  
    /************************************************************************************
     * Compare two tuples 
     *
     * @param 'a' elements of this tuple
     * @param 'b' elements of second table tuple;
     * @param 't_attrs' attribute key 1
     * @param 'u_attrs' attribute key 2
     * @param  'table2' importing table2 
     * @return @return boolean value- true or false
     * @author Hiten Nirmal
     * @author Niyati Shah
     */
    
    public boolean checkCommonElements(Comparable[] a, Comparable b[], String[] t_attrs, String[] u_attrs, Table table2)
    {	
      //This method is used of checking the element of matching attributed in both the tables and if the attributes 
      //are same it returns true else false.
	   Comparable[] extract1;
	   extract1 = extract(a,t_attrs);  
	   Comparable[] extract2;
	   extract2 = table2.extract(b,u_attrs);
	   //if (c.size() == 0) {
		 //  return false;
		   //}
	   for(int i=0;i<extract1.length;i++)
	   {
		   if(extract1[i].equals(extract2[i]))
			   continue;
		   else
			  return false;
	   }
	   return true;
    }
    
    public int getTableLength()
    {
    	return tuples.size();
    }
    
    public Comparable[] getTuple(int index)
    {
    	return tuples.get(index);
    }//get tuple for unit testing
    
} // Table class
