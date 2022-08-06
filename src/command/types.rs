pub struct Types;

impl Types {
    /**
     * <P>The pub const ant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>BIT</code>.
     */
    pub const  BIT   :i32          =  -7;

    /**
     * <P>The pub const ant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>TINYINT</code>.
     */
    pub const  TINYINT    :i32     =  -6;

    /**
     * <P>The pub const ant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>SMALLINT</code>.
     */
    pub const  SMALLINT    :i32    =   5;

    /**
     * <P>The pub const ant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>INTEGER</code>.
     */
    pub const  INTEGER     :i32    =   4;

    /**
     * <P>The pub const ant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>BIGINT</code>.
     */
    pub const  BIGINT      :i32    =  -5;

    /**
     * <P>The pub const ant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>FLOAT</code>.
     */
    pub const  FLOAT       :i32    =   6;

    /**
     * <P>The pub const ant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>REAL</code>.
     */
    pub const  REAL         :i32   =   7;


    /**
     * <P>The pub const ant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>DOUBLE</code>.
     */
    pub const  DOUBLE      :i32    =   8;

    /**
     * <P>The pub const ant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>NUMERIC</code>.
     */
    pub const  NUMERIC     :i32    =   2;

    /**
     * <P>The pub const ant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>DECIMAL</code>.
     */
    pub const  DECIMAL    :i32     =   3;

    /**
     * <P>The pub const ant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>CHAR</code>.
     */
    pub const  CHAR      :i32      =   1;

    /**
     * <P>The pub const ant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>VARCHAR</code>.
     */
    pub const  VARCHAR     :i32    =  12;

    /**
     * <P>The pub const ant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>LONGVARCHAR</code>.
     */
    pub const  LONGVARCHAR   :i32  =  -1;


    /**
     * <P>The pub const ant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>DATE</code>.
     */
    pub const  DATE      :i32      =  91;

    /**
     * <P>The pub const ant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>TIME</code>.
     */
    pub const  TIME        :i32    =  92;

    /**
     * <P>The pub const ant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>TIMESTAMP</code>.
     */
    pub const  TIMESTAMP   :i32    =  93;


    /**
     * <P>The pub const ant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>BINARY</code>.
     */
    pub const  BINARY     :i32     =  -2;

    /**
     * <P>The pub const ant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>VARBINARY</code>.
     */
    pub const  VARBINARY    :i32   =  -3;

    /**
     * <P>The pub const ant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>LONGVARBINARY</code>.
     */
    pub const  LONGVARBINARY :i32  =  -4;

    /**
     * <P>The pub const ant in the Java programming language
     * that identifies the generic SQL value
     * <code>NULL</code>.
     */
    pub const  NULL     :i32       =   0;

    /**
     * The pub const ant in the Java programming language that indicates
     * that the SQL type is database-specific and
     * gets mapped to a Java object that can be accessed via
     * the methods <code>getObject</code> and <code>setObject</code>.
     */
    pub const  OTHER    :i32       = 1111;



    /**
     * The pub const ant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>JAVA_OBJECT</code>.
     * @since 1.2
     */
    pub const  JAVA_OBJECT  :i32       = 2000;

    /**
     * The pub const ant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>DISTINCT</code>.
     * @since 1.2
     */
    pub const  DISTINCT    :i32        = 2001;

    /**
     * The pub const ant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>STRUCT</code>.
     * @since 1.2
     */
    pub const  STRUCT      :i32        = 2002;

    /**
     * The pub const ant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>ARRAY</code>.
     * @since 1.2
     */
    pub const  ARRAY      :i32         = 2003;

    /**
     * The pub const ant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>BLOB</code>.
     * @since 1.2
     */
    pub const  BLOB       :i32         = 2004;

    /**
     * The pub const ant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>CLOB</code>.
     * @since 1.2
     */
    pub const  CLOB       :i32         = 2005;

    /**
     * The pub const ant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>REF</code>.
     * @since 1.2
     */
    pub const  REF        :i32         = 2006;

    /**
     * The pub const ant in the Java programming language, somtimes referred to
     * as a type code, that identifies the generic SQL type <code>DATALINK</code>.
     *
     * @since 1.4
     */
    pub const  DATALINK :i32= 70;

    /**
     * The pub const ant in the Java programming language, somtimes referred to
     * as a type code, that identifies the generic SQL type <code>BOOLEAN</code>.
     *
     * @since 1.4
     */
    pub const  BOOLEAN :i32= 16;

    //------------------------- JDBC 4.0 -----------------------------------

    /**
     * The pub const ant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type <code>ROWID</code>
     *
     * @since 1.6
     *
     */
    pub const  ROWID :i32= -8;

    /**
     * The pub const ant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type <code>NCHAR</code>
     *
     * @since 1.6
     */
    pub const  NCHAR :i32= -15;

    /**
     * The pub const ant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type <code>NVARCHAR</code>.
     *
     * @since 1.6
     */
    pub const  NVARCHAR :i32= -9;

    /**
     * The pub const ant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type <code>LONGNVARCHAR</code>.
     *
     * @since 1.6
     */
    pub const  LONGNVARCHAR :i32= -16;

    /**
     * The pub const ant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type <code>NCLOB</code>.
     *
     * @since 1.6
     */
    pub const  NCLOB :i32= 2011;

    /**
     * The pub const ant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type <code>XML</code>.
     *
     * @since 1.6
     */
    pub const  SQLXML :i32= 2009;

    //--------------------------JDBC 4.2 -----------------------------

    /**
     * The pub const ant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type {@code REF CURSOR}.
     *
     * @since 1.8
     */
    pub const  REF_CURSOR :i32= 2012;

    /**
     * The pub const ant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * {@code TIME WITH TIMEZONE}.
     *
     * @since 1.8
     */
    pub const  TIME_WITH_TIMEZONE :i32= 2013;

    /**
     * The pub const ant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * {@code TIMESTAMP WITH TIMEZONE}.
     *
     * @since 1.8
     */
    pub const  TIMESTAMP_WITH_TIMEZONE :i32= 2014;
}