using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;

namespace FicheirosBBLearn
{
    class Users
    {
        public string EXTERNAL_PERSON_KEY { get; set; }
        public string DATA_SOURCE_KEY { get; set; }
        public string FIRSTNAME { get; set; }
        public string LASTNAME { get; set; }
        public string USER_ID { get; set; }
        public string PASSWD { get; set; }
        public string AVAILABLE_IND { get; set; }
        public string EMAIL { get; set; }
        public string GENDER { get; set; }
        public string MIDDLENAME { get; set; }
        public string INSTITUTION_ROLE { get; set; }
        public string ROW_STATUS { get; set; }
        public string STUDENT_ID { get; set; }
        public string SYSTEM_ROLE { get; set; }
    }

    class Courses
    {
        public string COURSE_ID { get; set; }
        public string COURSE_NAME { get; set; }
        public string DURATION { get; set; }
        public string END_DATE { get; set; }
        public string ROW_SATTUS { get; set; }
        public string START_DATE { get; set; }
        public string ACADEMIC_YEAR { get; set; }
    }

    class Organizations
    {
        public string EXTERNAL_ORGANIZATION_KEY { get; set; }
        public string ORGANIZATION_ID { get; set; }
        public string ORGANIZATION_NAME { get; set; }
        public string DESCRIPTION { get; set; }
        public string DURATION { get; set; }
        public string END_DATE { get; set; }
        public string EXTERNAL_ASSOCIATION_KEY { get; set; }
        public string PRIMARY_EXTERNAL_NODE_KEY { get; set; }
        public string ROW_STATUS { get; set; }
        public string TEMPLATE_ORGANIZATION_KEY { get; set; }
        public string START_DATE { get; set; }
        public string TERM_KEY { get; set; }
        public string ACADEMIC_YEAR { get; set; }
        public string ALLOW_GUEST_IND { get; set; }
        public string ALLOW_OBSERVER_IND { get; set; }
    }

    class CourseMembership
    {
        public string EXTERNAL_COURSE_KEY { get; set; }
        public string EXTERNAL_PERSON_KEY { get; set; }
        public string AVAILABLE_IND { get; set; }
        public string ROLE { get; set; }
        public string ROW_STATUS { get; set; }
        public string ACADEMIC_YEAR { get; set; }
    }

    class OrganizationMembership
    {
        public string EXTERNAL_ORGANIZATION_KEY { get; set; }
        public string DATA_SOURCE_KEY { get; set; }
        public string EXTERNAL_PERSON_KEY { get; set; }
        public string AVAILABLE_IND { get; set; }
        public string ROLE { get; set; }
        public string ROW_STATUS { get; set; }
        public string ACADEMIC_YEAR { get; set; }
    }

    class CourseAssociation
    {
        public string EXTERNAL_COURSE_KEY { get; set; }
        public string DATA_SOURCE_KEY { get; set; }
        public string EXTERNAL_NODE_KEY { get; set; }
    }

    class UsersAssociation
    {
        public string Data_SOURCE_KEY { get; set; }
        public string EXTERNAL_ASSOCIATION_KEY { get; set; }
        public string EXTERNAL_NODE_KEY { get; set; }
        public string EXTERNAL_USER_KEY { get; set; }
    }

    class SecInstRoleAssociation
    {
        public string DATA_SOURCE_KEY { get; set; }
        public string ROLE_ID { get; set; }
        public string EXTERNAL_PERSON_KEY { get; set; }
        public string ROW_Status { get; set; }
    }

    class Program
    {
        static List<OrganizationMembership> listaOrganizationsMembership = new List<OrganizationMembership>();

        static void Main(string[] args)
        {
            string str = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
            string strIADE = ConfigurationManager.ConnectionStrings["connectionStringIADE"].ConnectionString;
            string strIPAMLX = ConfigurationManager.ConnectionStrings["connectionStringIPAMLX"].ConnectionString;
            string strIPAMPRT = ConfigurationManager.ConnectionStrings["connectionStringIPAMPRT"].ConnectionString;
            string strSophisBB = ConfigurationManager.ConnectionStrings["connectionStringSophisBB"].ConnectionString;

            int ano = 2016;

            List<Users> listaUsers = GetUsers(str, strIADE, strIPAMLX, strIPAMPRT, ano);
            List<Courses> listaCourses = GetCourses(strSophisBB, ano);
            //List<Courses> listaCourses = GetCourses(str, strIADE, strIPAMLX, strIPAMPRT, ano);
            List<CourseMembership> listaInscricoes = GetCourseMembership(strSophisBB, ano);
            //List<CourseMembership> listaInscricoes = GetCourseMembership(str,strIADE, strIPAMLX, strIPAMPRT, ano);
            List<CourseAssociation> listaCourseAssociation = GetCourseAssociation(strSophisBB, ano);
            //List<CourseAssociation> listaCourseAssociation = GetCourseAssociation(str, strIADE, strIPAMLX, strIPAMPRT, ano);
            List<UsersAssociation> listaUsersAssociation = GetUsersAssociations(str,strIADE, strIPAMLX, strIPAMPRT, ano);
            List<SecInstRoleAssociation> listaSecInstRole = GetSecInstRoleAssociations(str, strIADE, strIPAMLX, strIPAMPRT, ano);
            List<Organizations> listaOrganizations = new List<Organizations>();
            

            CreateFiles();
            WriteUsers(listaUsers);
            WriteCourses(listaCourses);
            WriteCouresesMembership(listaInscricoes);
            WriteCoursesAssociations(listaCourseAssociation);
            WriteUsersAssociations(listaUsersAssociation);
            WriteSecInstRoleAssociations(listaSecInstRole);
            WriteCategories();
            WritehierarchyNodes();
            WriteOrganizations();
            writeOrganizationsMembership(listaOrganizationsMembership);
        }

        #region Get Users
        public static List<Users> GetUsers(string str,string strIADE, string strIPAMLX, string strIPAMPRT, int ano)
        {
            List<Users> listaUsers = new List<Users>();
            List<Users> listaTemp = new List<Users>();
            
            /* -- Importação Estudantes UE - INICIO --*/
            SqlConnection conn = new SqlConnection(str);
            listaUsers = GetEstudantes(conn, ano, "PTEU01.");

            foreach(Users temp in listaUsers)
            {
                SetOrganizationMembership("PTEU01.PTEU01-003", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano);
                SetOrganizationMembership("PTEU01.PTEU01-008", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Academia Competencias
                SetOrganizationMembership("PTEU01.PTEU01-009", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Biblioteca
                SetOrganizationMembership("PTEU01.PTEU01-012", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Vida Académica
                SetOrganizationMembership("PTEU01.PTEU01-022", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Empregabiliade
                SetOrganizationMembership("PTEU01.PTEU01-013", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Recursos IT
            }
            /* -- Importação Estudantes UE - FIM --*/

            /* -- Importação Docentes UE - INICIO --*/
            listaTemp = GetDocentes(conn, ano, "PTEU01.", "");
            foreach (Users u in listaTemp)
            {
                listaUsers.Add(u);
            }

            foreach(Users temp in listaTemp)
            {
                SetOrganizationMembership("PTEU01.PTEU01-002", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano);
                SetOrganizationMembership("PTEU01.PTEU01-008", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Academia Competencias
                SetOrganizationMembership("PTEU01.PTEU01-009", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Biblioteca
                SetOrganizationMembership("PTEU01.PTEU01-012", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Vida Académica
                SetOrganizationMembership("PTEU01.PTEU01-022", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Empregabiliade
                SetOrganizationMembership("PTEU01.PTEU01-013", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Recursos IT
            }
            /* -- Importação Docentes UE - FIM --*/

            /* -- Importação Estudantes IADE - INICIO --*/
            SqlConnection connIADE = new SqlConnection(strIADE);
            listaTemp = GetEstudantes(connIADE, ano, "PTEU01.");
            foreach (Users u in listaTemp)
            {
                listaUsers.Add(u);
            }
            foreach (Users temp in listaTemp)
            {
                SetOrganizationMembership("PTEU01.PTIA01-002", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano);
                SetOrganizationMembership("PTEU01.PTEU01-008", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Academia Competencias
                SetOrganizationMembership("PTEU01.PTEU01-009", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Biblioteca
                SetOrganizationMembership("PTEU01.PTEU01-012", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Vida Académica
                SetOrganizationMembership("PTEU01.PTEU01-022", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Empregabiliade
                SetOrganizationMembership("PTEU01.PTEU01-013", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Recursos IT
            }
            /* -- Importação Estudantes IADE - FIM --*/

            /* -- Importação Docentes IADE - INICIO --*/
            listaTemp = GetDocentes(connIADE, ano, "PTEU01.", "");
            foreach (Users u in listaTemp)
            {
                listaUsers.Add(u);
            }
            foreach (Users temp in listaTemp)
            {
                SetOrganizationMembership("PTEU01.PTIA01-001", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano);
                SetOrganizationMembership("PTEU01.PTEU01-008", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Academia Competencias
                SetOrganizationMembership("PTEU01.PTEU01-009", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Biblioteca
                SetOrganizationMembership("PTEU01.PTEU01-012", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Vida Académica
                SetOrganizationMembership("PTEU01.PTEU01-022", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Empregabiliade
                SetOrganizationMembership("PTEU01.PTEU01-013", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Recursos IT
            }
            /* -- Importação Docentes IADE - FIM --*/

            /* -- Importação Estudantes IPAM LX - INICIO --*/
            SqlConnection connIPAMLX = new SqlConnection(strIPAMLX);
            listaTemp = GetEstudantes(connIPAMLX, ano, "PTEU01.");
            foreach (Users u in listaTemp)
            {
                listaUsers.Add(u);
            }
            foreach (Users temp in listaTemp)
            {
                SetOrganizationMembership("PTEU01.PTIP01-004", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano);
                SetOrganizationMembership("PTEU01.PTEU01-008", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Academia Competencias
                SetOrganizationMembership("PTEU01.PTEU01-009", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Biblioteca
                SetOrganizationMembership("PTEU01.PTEU01-012", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Vida Académica
                SetOrganizationMembership("PTEU01.PTEU01-022", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Empregabiliade
                SetOrganizationMembership("PTEU01.PTEU01-013", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Recursos IT
            }
            /* -- Importação Estudantes IPAM LX - FIM --*/

            /* -- Importação Docentes IPAM LX - INICIO --*/
            listaTemp = GetDocentes(connIPAMLX, ano, "PTEU01.", "");
            foreach (Users u in listaTemp)
            {
                listaUsers.Add(u);
            }
            foreach (Users temp in listaTemp)
            {
                SetOrganizationMembership("PTEU01.PTIP01-003", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano);
                SetOrganizationMembership("PTEU01.PTEU01-008", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Academia Competencias
                SetOrganizationMembership("PTEU01.PTEU01-009", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Biblioteca
                SetOrganizationMembership("PTEU01.PTEU01-012", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Vida Académica
                SetOrganizationMembership("PTEU01.PTEU01-022", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Empregabiliade
                SetOrganizationMembership("PTEU01.PTEU01-013", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Recursos IT
            }
            /* -- Importação Docentes IPAM LX - FIM --*/

            /* -- Importação Estudantes IPAM PORTO - INICIO --*/
            SqlConnection connIPAMPRT = new SqlConnection(strIPAMPRT);
            listaTemp = GetEstudantes(connIPAMPRT, ano, "PTEU01.");
            foreach(Users u in listaTemp)
            {
                u.EXTERNAL_PERSON_KEY = u.EXTERNAL_PERSON_KEY + "@ipam.pt";
                u.USER_ID = u.USER_ID + "@ipam.pt";
            }
            foreach (Users u in listaTemp)
            {
                listaUsers.Add(u);
            }
            foreach (Users temp in listaTemp)
            {
                SetOrganizationMembership("PTEU01.PTIP01-002", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano);
                SetOrganizationMembership("PTEU01.PTEU01-008", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Academia Competencias
                SetOrganizationMembership("PTEU01.PTEU01-009", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Biblioteca
                SetOrganizationMembership("PTEU01.PTEU01-012", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Vida Académica
                SetOrganizationMembership("PTEU01.PTEU01-022", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Empregabiliade
                SetOrganizationMembership("PTEU01.PTEU01-013", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Recursos IT
            }
            /* -- Importação Estudantes IPAM PORTO - FIM --*/

            /* -- Importação Docentes IPAM Porto - INICIO --*/
            listaTemp = GetDocentes(connIPAMPRT, ano, "PTEU01.", "");
            foreach (Users u in listaTemp)
            {
                listaUsers.Add(u);
            }
            foreach (Users temp in listaTemp)
            {
                SetOrganizationMembership("PTEU01.PTIP01-001", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano);
                SetOrganizationMembership("PTEU01.PTEU01-008", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Academia Competencias
                SetOrganizationMembership("PTEU01.PTEU01-009", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Biblioteca
                SetOrganizationMembership("PTEU01.PTEU01-012", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Vida Académica
                SetOrganizationMembership("PTEU01.PTEU01-022", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Empregabiliade
                SetOrganizationMembership("PTEU01.PTEU01-013", temp.EXTERNAL_PERSON_KEY, "PTEU01.Student", ano); //Recursos IT
            }
            /* -- Importação Docentes IPAM Porto - FIM --*/
            return listaUsers;
        }
        #endregion

        #region Get Estudantes
        private static List<Users> GetEstudantes(SqlConnection conn, int ano, string EPK)
        {
            List<Users> listaUsers = new List<Users>();
            string queryUsers = "SELECT DISTINCT [RAlc_Cp_NAluno],[Enti_NomeProprio],[Enti_Apelido],[Enti_Email],[Enti_Sexo],[RAlc_Estado],[RAlc_UltAnoLect] FROM[dbo].[TRAluCur] INNER JOIN TEntidad ON TRAluCur.RAlc_Ce_CdEntidade = TEntidad.Enti_Cp_CdEntidade WHERE[RAlc_UltAnoLect] > 2015 AND([RAlc_Estado] = 'I' OR [RAlc_Estado] = 'IA')";
            //string queryUsers = "SELECT DISTINCT[Insc_Cx_CdAluno] ,[Enti_NomeProprio] ,[Enti_Apelido] ,[Enti_Email] ,[Enti_Sexo] ,[Insc_ValidInsc] ,[Insc_Cp_AnoLect] FROM[dbo].[TInscri] INNER JOIN(TRAluCur INNER JOIN TEntidad ON TRAluCur.RAlc_Ce_CdEntidade = TEntidad.Enti_Cp_CdEntidade) ON[TInscri].[Insc_Cx_CdAluno] = TRAluCur.RAlc_Cp_NAluno Where[Insc_Cp_AnoLect] > " + ano;
            SqlCommand cmd = new SqlCommand(queryUsers, conn);

            conn.Open();
            SqlDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                Users novoUser = new Users();

                try { novoUser.EXTERNAL_PERSON_KEY = EPK + Convert.ToString(dr.GetValue(0)); } catch (SqlException ex) {/*TODO write to logs*/ };
                novoUser.DATA_SOURCE_KEY = "";
                try
                {
                    novoUser.FIRSTNAME = Convert.ToString(dr.GetValue(1));
                    if (novoUser.FIRSTNAME == "") novoUser.FIRSTNAME = "Blank";
                }
                catch (SqlException ex) {/*TODO write to logs*/ };
                try
                {
                    novoUser.LASTNAME = Convert.ToString(dr.GetValue(2));
                    if (novoUser.LASTNAME == "") novoUser.LASTNAME = "blank";
                }
                catch (SqlException ex) {/*TODO write to logs*/ };
                try { novoUser.USER_ID = Convert.ToString(dr.GetValue(0)); } catch (SqlException ex) {/*TODO write to logs*/ };
                novoUser.PASSWD = "";
                try
                {
                    string temp = Convert.ToString(dr.GetValue(5));
                    if (temp == "I" || temp == "IA") novoUser.AVAILABLE_IND = "Y";
                    else novoUser.AVAILABLE_IND = "N";
                }
                catch (SqlException ex) { /*TODO write to logs*/};
                try { novoUser.EMAIL = Convert.ToString(dr.GetValue(3)); } catch (SqlException ex) {/*TODO write to logs*/ };
                try
                {
                    string temp = Convert.ToString(dr.GetValue(4));
                    if (temp == "M") novoUser.GENDER = "Male";
                    if (temp == "F") novoUser.GENDER = "Female";
                }
                catch (SqlException ex) { /*TODO write to logs*/};
                novoUser.MIDDLENAME = "";
                novoUser.INSTITUTION_ROLE = "";
                try
                {
                    string temp = Convert.ToString(dr.GetValue(5));
                    if (temp == "I" || temp == "IA") novoUser.ROW_STATUS = "enabled";
                    else novoUser.ROW_STATUS = "disabled";
                }
                catch (SqlException ex) { /*TODO write to logs*/};
                try { novoUser.STUDENT_ID = Convert.ToString(dr.GetValue(0)); } catch (SqlException ex) {/*TODO write to logs*/ };
                novoUser.SYSTEM_ROLE = "";

                listaUsers.Add(novoUser);
            }
            conn.Close();
            return listaUsers;
        }
        #endregion

        #region Get Docentes
        private static List<Users> GetDocentes(SqlConnection conn, int ano, string EPK, string campoTemp)
        {
            List<Users> listaUsers = new List<Users>();

            string queryUsers = "SELECT[Clie_Cp_CdCliTes],[Clie_NmCliTes],[Clie_EMail],[Clie_Sexo],[Clie_Estado],Substring([Clie_NmCliTes],1, charindex(' ', [Clie_NmCliTes])) as primeiro_nome, CASE WHEN CHARINDEX(' ', REVERSE(RTRIM(LTRIM([Clie_NmCliTes])))) < 1 THEN[Clie_NmCliTes] ELSE LTRIM(RTRIM(RIGHT(LTRIM(RTRIM([Clie_NmCliTes])), CHARINDEX(' ', REVERSE(RTRIM(LTRIM([Clie_NmCliTes]))))))) END as ultimo_nome FROM[bdsophis].[dbo].[TCliTes] where[Clie_Ce_TpCliTes] = 5 --AND Clie_Estado = 'A'";
            SqlCommand cmd = new SqlCommand(queryUsers, conn);

            conn.Open();
            SqlDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                Users novoUser = new Users();

                try { novoUser.EXTERNAL_PERSON_KEY = EPK + campoTemp + Convert.ToString(dr.GetValue(0)); } catch (SqlException ex) {/*TODO write to logs*/ };
                novoUser.DATA_SOURCE_KEY = "";
                try { novoUser.FIRSTNAME = Convert.ToString(dr.GetValue(5)); } catch (SqlException ex) {/*TODO write to logs*/ };
                try { novoUser.LASTNAME = Convert.ToString(dr.GetValue(6)); } catch (SqlException ex) {/*TODO write to logs*/ };
                try { novoUser.USER_ID = campoTemp + Convert.ToString(dr.GetValue(0)); } catch (SqlException ex) {/*TODO write to logs*/ };
                novoUser.PASSWD = "";
                //perguntar qual é o significado do campo Available_ind
                try
                {
                    string temp = Convert.ToString(dr.GetValue(4));
                    if (temp == "A") novoUser.AVAILABLE_IND = "Y";
                    else novoUser.AVAILABLE_IND = "N";
                }
                catch (SqlException ex) { /*TODO write to logs*/};
                try { novoUser.EMAIL = Convert.ToString(dr.GetValue(2)); } catch (SqlException ex) {/*TODO write to logs*/ };
                try
                {
                    string temp = Convert.ToString(dr.GetValue(3));
                    if (temp == "M") novoUser.GENDER = "Male";
                    if (temp == "F") novoUser.GENDER = "Female";
                }
                catch (SqlException ex) { /*TODO write to logs*/};
                novoUser.MIDDLENAME = "";
                novoUser.INSTITUTION_ROLE = "";
                try
                {
                    string temp = Convert.ToString(dr.GetValue(4));
                    if (temp == "A") novoUser.ROW_STATUS = "enabled";
                    else novoUser.ROW_STATUS = "disabled";
                }
                catch (SqlException ex) { /*TODO write to logs*/};
                try { novoUser.STUDENT_ID = Convert.ToString(dr.GetValue(0)); } catch (SqlException ex) {/*TODO write to logs*/ };
                novoUser.SYSTEM_ROLE = "";

                listaUsers.Add(novoUser);
            }
            conn.Close();

            return listaUsers;
        }
        #endregion

        #region Set Membership
        private static void SetOrganizationMembership(string EOK, string EPK, string ROLE, int ano)
        {
            OrganizationMembership temp = new OrganizationMembership();
            temp.EXTERNAL_ORGANIZATION_KEY = EOK;
            temp.DATA_SOURCE_KEY = "";
            temp.EXTERNAL_PERSON_KEY = EPK;
            temp.AVAILABLE_IND = "Y";
            temp.ROLE = ROLE;
            temp.ROW_STATUS = "enabled";
            temp.ACADEMIC_YEAR = Convert.ToString(ano + 1);

            listaOrganizationsMembership.Add(temp);
        }
        #endregion



        #region Get Courses
        public static List<Courses> GetCourses(string str, int ano)
        {
            List<Courses> listaCourses = new List<Courses>();

            SqlConnection conn = new SqlConnection(str);
            string query = "select * from View_Insc_BB_Courses";
            SqlCommand cmd = new SqlCommand(query, conn);

            conn.Open();
            SqlDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                Courses novoCourse = new Courses();
                try
                {
                    novoCourse.COURSE_ID = Convert.ToString(dr.GetValue(0));
                    novoCourse.COURSE_NAME = Convert.ToString(dr.GetValue(2));
                    novoCourse.DURATION = Convert.ToString(dr.GetValue(5));
                    novoCourse.END_DATE = Convert.ToString(dr.GetValue(6));
                    novoCourse.ROW_SATTUS = Convert.ToString(dr.GetValue(9));
                    novoCourse.START_DATE = Convert.ToString(dr.GetValue(11));
                    novoCourse.ACADEMIC_YEAR = Convert.ToString(dr.GetValue(12));

                    listaCourses.Add(novoCourse);
                }
                catch (Exception ex) { /*TODO write to logs*/};
            }
            conn.Close();

            return listaCourses;
        }

        public static List<Courses> GetCourses(string str,string strIADE, string strIPAMLX, string strIPAMPRT, int ano)
        {
            List<Courses> listaCourses = new List<Courses>();
            List<Courses> listaTemp = new List<Courses>();

            /* -- Importação UCs UE - INICIO --*/
            SqlConnection conn = new SqlConnection(str);
            listaCourses = GetUCs(conn, ano);
            /* -- Importação UCs UE - INICIO --*/

            /* -- Importação UCs IADE - INICIO --*/
            SqlConnection connIADE = new SqlConnection(strIADE);
            listaTemp = GetUCs(connIADE, ano);
            foreach(Courses c in listaTemp)
            {
                listaCourses.Add(c);
            }
            /* -- Importação UCs IADE - FIM --*/

            /* -- Importação UCs IPAM LX - INICIO --*/
            SqlConnection connIPAMLX = new SqlConnection(strIPAMLX);
            listaTemp = GetUCs(connIPAMLX, ano);
            foreach (Courses c in listaTemp)
            {
                listaCourses.Add(c);
            }
            /* -- Importação UCs IPAM LX - FIM --*/

            /* -- Importação UCs IPAM Porto - INICIO --*/
            SqlConnection connIPAMPRT = new SqlConnection(strIPAMPRT);
            listaTemp = GetUCs(connIPAMPRT, ano);
            foreach (Courses c in listaTemp)
            {
                listaCourses.Add(c);
            }
            /* -- Importação UCs IPAM Porto - FIM --*/

            return listaCourses;
        }
        #endregion

        #region GetUCs
        private static List<Courses> GetUCs(SqlConnection conn, int ano)
        {
            List<Courses> listaCourses = new List<Courses>();

            string query = "SELECT [Hcad_Cx_CdDepartamento] ,[Hcad_Cx_CdCadeira] ,[Hcad_Cp_NHorario], Cade_NmCadeira ,[Hcad_Ce_AnoLect], Hcad_DgHorario FROM[bdsophis].[dbo].[THCad] INNER JOIN TCadeira ON[Hcad_Cx_CdDepartamento] = Cade_Cx_CdDepartamento AND[Hcad_Cx_CdCadeira] = Cade_Cp_CdCadeira WHERE[Hcad_Ce_AnoLect] > " + ano;
            //string query = "SELECT DISTINCT [Insc_Cx_CdDepartamento] ,[Insc_Cx_CdCadeira] ,[Insc_NHorario] ,TCadeira.Cade_NmCadeira  ,[Insc_Cp_AnoLect]  FROM[bdsophis].[dbo].[TInscri]  INNER JOIN TCadeira ON[Insc_Cx_CdDepartamento] = Cade_Cx_CdDepartamento AND[Insc_Cx_CdCadeira] = Cade_Cp_CdCadeira  WHERE[Insc_Cp_AnoLect] > " + ano;
            //string query = "SELECT DISTINCT [Insc_Cx_CdDepartamento],[Insc_Cx_CdCadeira],[Cade_NmCadeira],[Insc_Cp_AnoLect],[Insc_Cx_CdPeriod],[Insc_NHorario],Convert(varchar,Cale_Cp_DtInicio,112),Convert(varchar,Cale_DtFim,112),[Insc_ValidInsc] FROM[dbo].[TInscri] INNER JOIN TCadeira ON[Insc_Cx_CdDepartamento] = [Cade_Cx_CdDepartamento] AND[Insc_Cx_CdCadeira] = [Cade_Cp_CdCadeira] INNER JOIN[TCalendAulas] ON[Insc_Ce_CdCurso] = [Cale_Cx_CdCurso] AND[Insc_Cp_AnoLect] = [Cale_Cp_AnoLect] WHERE[Insc_Cp_AnoLect] >" + ano;
            SqlCommand cmd = new SqlCommand(query, conn);

            conn.Open();
            SqlDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                Courses novoCourse = new Courses();
                try
                {
                    string anoLetivo = Convert.ToString(dr.GetValue(4));
                    string cdDepart = Convert.ToString(dr.GetValue(0));
                    string cdUC = Convert.ToString(dr.GetValue(1));
                    string cdHorario = Convert.ToString(dr.GetValue(2));
                    novoCourse.COURSE_ID = anoLetivo + "_" + cdDepart + "C" + cdUC + "_" + cdHorario;
                }
                catch (Exception ex) { /*TODO write to logs*/};

                try { novoCourse.COURSE_NAME = Convert.ToString(dr.GetValue(3)) + " " + Convert.ToString(dr.GetValue(5)); } catch (Exception ex) { /*TODO write to logs*/};
                try
                {
                    novoCourse.END_DATE = "";// Convert.ToString(dr.GetValue(7));
                }
                catch (Exception ex) { /*TODO write to logs*/};
                try
                {
                    string temp = "S";// Convert.ToString(dr.GetValue(8));
                    if (temp == "S") novoCourse.ROW_SATTUS = "enabled";
                    else novoCourse.ROW_SATTUS = "disabled";
                }
                catch (Exception ex) { /*TODO write to logs*/};
                try { novoCourse.START_DATE = Convert.ToString(ano + 1) + "1001"; /* Convert.ToString(dr.GetValue(6));*/ } catch (Exception ex) { /*TODO write to logs*/};
                try { novoCourse.ACADEMIC_YEAR = Convert.ToString(ano + 1);/*Convert.ToString(dr.GetValue(3));*/ } catch (Exception ex) { /*TODO write to logs*/};

                listaCourses.Add(novoCourse);
            }
            conn.Close();

            return listaCourses;
        }
        #endregion



        #region Get Course Membership
        public static List<CourseMembership> GetCourseMembership(string strSophisBB, int ano)
        {
            List<CourseMembership> listaInscricoes = new List<CourseMembership>();
            List<CourseMembership> listaTemp = new List<CourseMembership>();

            SqlConnection conn = new SqlConnection(strSophisBB);
            listaInscricoes = GetStudentMembershipSophisBB(conn, ano);

            foreach (CourseMembership cm in listaInscricoes)
            {
                if (cm.EXTERNAL_COURSE_KEY.StartsWith("PTEU01.2000"))
                {
                    cm.EXTERNAL_PERSON_KEY = cm.EXTERNAL_PERSON_KEY + "@ipam.pt";
                }
            }

            listaTemp = GetTeaacherMembershipSophisBB(conn, ano);
            foreach (CourseMembership cm in listaTemp)
            {
                listaInscricoes.Add(cm);
            }

            return listaInscricoes;
        }
   
        private static List<CourseMembership> GetStudentMembershipSophisBB(SqlConnection conn, int ano)
        {
            List<CourseMembership> listaIncricoes = new List<CourseMembership>();

            string query = "select * from Insc_BB_Aluno_chave where Academic_year > " + ano;
            SqlCommand cmd = new SqlCommand(query, conn);

            conn.Open();
            SqlDataReader dr = cmd.ExecuteReader();
            while(dr.Read())
            {
                CourseMembership novaInscricao = new CourseMembership();
                try
                {
                    novaInscricao.EXTERNAL_COURSE_KEY = Convert.ToString(dr.GetValue(0));
                    novaInscricao.EXTERNAL_PERSON_KEY = Convert.ToString(dr.GetValue(2));
                    novaInscricao.AVAILABLE_IND = Convert.ToString(dr.GetValue(3));
                    novaInscricao.ROLE = Convert.ToString(dr.GetValue(4));
                    novaInscricao.ROW_STATUS = Convert.ToString(dr.GetValue(5));
                    novaInscricao.ACADEMIC_YEAR = Convert.ToString(dr.GetValue(6));

                    listaIncricoes.Add(novaInscricao);
                }
                catch(SqlException ex)
                {

                }
            }
            conn.Close();

            query = "select * from Course_Memberships where ACADEMIC_YEAR <" + (ano + 1);
            cmd = new SqlCommand(query, conn);

            conn.Open();
           SqlDataReader dr2 = cmd.ExecuteReader();
            while (dr2.Read())
            {
                CourseMembership novaInscricao = new CourseMembership();
                try
                {
                    novaInscricao.EXTERNAL_COURSE_KEY = Convert.ToString(dr2.GetValue(0));
                    novaInscricao.EXTERNAL_PERSON_KEY = Convert.ToString(dr2.GetValue(2));
                    novaInscricao.AVAILABLE_IND = Convert.ToString(dr2.GetValue(3));
                    novaInscricao.ROLE = Convert.ToString(dr2.GetValue(4));
                    novaInscricao.ROW_STATUS = Convert.ToString(dr2.GetValue(5));
                    novaInscricao.ACADEMIC_YEAR = Convert.ToString(dr2.GetValue(6));

                    listaIncricoes.Add(novaInscricao);
                }
                catch (SqlException ex)
                {

                }
            }
            conn.Close();

            return listaIncricoes;
        }

        private static List<CourseMembership> GetTeaacherMembershipSophisBB(SqlConnection conn, int ano)
        {
            List<CourseMembership> listaIncricoes = new List<CourseMembership>();

            string query = "select * from Insc_BB_docente_chave where Academic_year > " + ano;
            SqlCommand cmd = new SqlCommand(query, conn);

            conn.Open();
            SqlDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                CourseMembership novaInscricao = new CourseMembership();
                try
                {
                    novaInscricao.EXTERNAL_COURSE_KEY = Convert.ToString(dr.GetValue(0));
                    novaInscricao.EXTERNAL_PERSON_KEY = Convert.ToString(dr.GetValue(2));
                    novaInscricao.AVAILABLE_IND = Convert.ToString(dr.GetValue(3));
                    novaInscricao.ROLE = Convert.ToString(dr.GetValue(4));
                    novaInscricao.ROW_STATUS = Convert.ToString(dr.GetValue(5));
                    novaInscricao.ACADEMIC_YEAR = Convert.ToString(dr.GetValue(6));

                    listaIncricoes.Add(novaInscricao);
                }
                catch (SqlException ex)
                {

                }
            }
            conn.Close();

            return listaIncricoes;
        }

        public static List<CourseMembership> GetCourseMembership(string str, string strIADE, string strIPAMLX, string strIPAMPRT, int ano)
        {
            List<CourseMembership> listaInscricoes = new List<CourseMembership>();
            List<CourseMembership> listaTemp = new List<CourseMembership>();

            /* -- Importação Inscrições UCs UE - INICIO --*/
            SqlConnection conn = new SqlConnection(str);
            listaInscricoes = GetStudentMembership(conn, ano, "PTEU01.");
            listaTemp = GetTeacherMembership(conn, ano, "PTEU01.", "");
            foreach(CourseMembership cm in listaTemp)
            {
                listaInscricoes.Add(cm);
            }
            /* -- Importação Inscrições UCs UE - FIM --*/

            /* -- Importação Inscrições UCs IADE - INICIO --*/
            SqlConnection connIADE = new SqlConnection(strIADE);
            listaTemp = GetStudentMembership(connIADE, ano, "PTEU01.");
            foreach (CourseMembership cm in listaTemp)
            {
                listaInscricoes.Add(cm);
            }
            listaTemp = GetTeacherMembership(connIADE, ano, "PTEU01.", "");
            foreach (CourseMembership cm in listaTemp)
            {
                listaInscricoes.Add(cm);
            }
            /* -- Importação Inscrições UCs IADE - FIM --*/

            /* -- Importação Inscrições UCs IPAM LX - INICIO --*/
            SqlConnection connIPAMLX = new SqlConnection(strIPAMLX);
            listaTemp = GetStudentMembership(connIPAMLX, ano, "PTEU01.");
            foreach (CourseMembership cm in listaTemp)
            {
                listaInscricoes.Add(cm);
            }
            listaTemp = GetTeacherMembership(connIPAMLX, ano, "PTEU01.", "");
            foreach (CourseMembership cm in listaTemp)
            {
                listaInscricoes.Add(cm);
            }
            /* -- Importação Inscrições UCs IPAM LX - FIM --*/

            /* -- Importação Inscrições UCs IPAM Porto - INICIO --*/
            SqlConnection connIPAMPRT = new SqlConnection(strIPAMPRT);
            listaTemp = GetStudentMembership(connIPAMPRT, ano, "PTEU01.");
            foreach(CourseMembership cm in listaTemp)
            {
                cm.EXTERNAL_PERSON_KEY = cm.EXTERNAL_PERSON_KEY + "@ipam.pt";
            }
            foreach (CourseMembership cm in listaTemp)
            {
                listaInscricoes.Add(cm);
            }
            listaTemp = GetTeacherMembership(connIPAMPRT, ano, "PTEU01.","");
            foreach (CourseMembership cm in listaTemp)
            {
                listaInscricoes.Add(cm);
            }
            /* -- Importação Inscrições UCs IPAM Porto - FIM --*/

            return listaInscricoes;
        }
        #endregion

        #region Get Student's Membership
        private static List<CourseMembership> GetStudentMembership(SqlConnection conn, int ano, string EPK)
        {
            List<CourseMembership> listaInscricoes = new List<CourseMembership>();

            string query = "SELECT Distinct [Insc_Cx_CdAluno],[Insc_Cx_CdDepartamento],[Insc_Cx_CdCadeira],[Insc_Cp_AnoLect],[Insc_NHorario],[Insc_ValidInsc] FROM[dbo].[TInscri] inner join THCad on TInscri.Insc_Cx_CdDepartamento = thcad.Hcad_Cx_CdDepartamento and TInscri.Insc_Cx_CdCadeira = thcad.Hcad_Cx_CdCadeira and tinscri.Insc_NHorario = thcad.Hcad_Cp_NHorario inner join tlhcad on[Lhca_Cx_CdDepartamento] = [Hcad_Cx_CdDepartamento] AND[Lhca_Cx_CdCadeira] = [Hcad_Cx_CdCadeira] AND[Lhca_Cx_NHorario] = [Hcad_Cp_NHorario] WHERE[Insc_Cp_AnoLect] >" + ano;
            SqlCommand cmd = new SqlCommand(query, conn);

            conn.Open();
            SqlDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                CourseMembership novaInscricao = new CourseMembership();

                try
                {
                    string anoLetivo = Convert.ToString(ano + 1);
                    string depart = Convert.ToString(dr.GetValue(1));
                    string uc = Convert.ToString(dr.GetValue(2));
                    string turma = Convert.ToString(dr.GetValue(4));
                    novaInscricao.EXTERNAL_COURSE_KEY = EPK + anoLetivo + "_" + depart + "C" + uc + "_" + turma;
                }
                catch (SqlException ex) {/*TODO write to logs*/ };
                try { novaInscricao.EXTERNAL_PERSON_KEY = EPK + Convert.ToString(dr.GetValue(0)); } catch (SqlException ex) {/*TODO write to logs*/ };
                try
                {
                    string temp = Convert.ToString(dr.GetValue(5));
                    if (temp == "S")
                    {
                        novaInscricao.AVAILABLE_IND = "Y";
                        novaInscricao.ROW_STATUS = "enabled";
                    }
                    else
                    {
                        novaInscricao.AVAILABLE_IND = "N";
                        novaInscricao.ROW_STATUS = "disabled";
                    }
                }
                catch (SqlException ex) {/*TODO write to logs*/ };
                novaInscricao.ROLE = EPK + "Student";
                try { novaInscricao.ACADEMIC_YEAR = Convert.ToString(dr.GetValue(3)); } catch (SqlException ex) {/*TODO write to logs*/ };

                listaInscricoes.Add(novaInscricao);
            }
            conn.Close();

            return listaInscricoes;
        }
        #endregion

        #region Get Teacher's Membership
        private static List<CourseMembership> GetTeacherMembership(SqlConnection conn, int ano, string EPK, string campoTem)
        {
            List<CourseMembership> listaInscricoes = new List<CourseMembership>();

            string query = "SELECT[Lhca_Cx_CdDepartamento],[Lhca_Cx_CdCadeira],[Lhca_Cx_NHorario],[Lhca_Ce_CdDocente] FROM[bdsophis].[dbo].[TLHCad] INNER JOIN[THCad] ON[Lhca_Cx_CdDepartamento] = [Hcad_Cx_CdDepartamento] AND[Lhca_Cx_CdCadeira] = [Hcad_Cx_CdCadeira] AND[Lhca_Cx_NHorario] = [Hcad_Cp_NHorario] WHERE[Hcad_Ce_AnoLect] >" + ano;

            SqlCommand cmd = new SqlCommand(query, conn);

            conn.Open();
            SqlDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                CourseMembership novaInscricao = new CourseMembership();

                try
                {
                    string anoLetivo = Convert.ToString(ano + 1);
                    string depart = Convert.ToString(dr.GetValue(0));
                    string uc = Convert.ToString(dr.GetValue(1));
                    string turma = Convert.ToString(dr.GetValue(2));
                    novaInscricao.EXTERNAL_COURSE_KEY = EPK + anoLetivo + "_" + depart + "C" + uc + "_" + turma;
                }
                catch (SqlException ex) {/*TODO write to logs*/ };
                try { novaInscricao.EXTERNAL_PERSON_KEY = EPK + campoTem + Convert.ToString(dr.GetValue(3)); } catch (SqlException ex) {/*TODO write to logs*/ };
                novaInscricao.AVAILABLE_IND = "Y";
                novaInscricao.ROW_STATUS = "enabled";
                novaInscricao.ROLE = EPK + "Teacher";
                novaInscricao.ACADEMIC_YEAR = Convert.ToString(ano + 1);
                listaInscricoes.Add(novaInscricao);
            }

            conn.Close();

            return listaInscricoes;
        }
        #endregion



        #region Get Course Associations
        public static List<CourseAssociation> GetCourseAssociation(string str, int ano)
        {
            List<CourseAssociation> listaCourseAssociations = new List<CourseAssociation>();

            SqlConnection conn = new SqlConnection(str);
            string query = "select * from View_Insc_BB_CourseKey";
            SqlCommand cmd = new SqlCommand(query, conn);

            conn.Open();
            SqlDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                CourseAssociation novaAssociacao = new CourseAssociation();
                try
                {
                    novaAssociacao.EXTERNAL_COURSE_KEY = Convert.ToString(dr.GetValue(0));
                    novaAssociacao.DATA_SOURCE_KEY = Convert.ToString(dr.GetValue(1));
                    novaAssociacao.EXTERNAL_NODE_KEY = Convert.ToString(dr.GetValue(2));

                    listaCourseAssociations.Add(novaAssociacao);
                }
                catch (SqlException ex) {/*TODO write to logs*/ };
            }
            conn.Close();

            return listaCourseAssociations;
        }

        public static List<CourseAssociation> GetCourseAssociation(string str,string strIADE, string strIPAMLX, string strIPAMPRT, int ano)
        {
            List<CourseAssociation> listaCourseAssociations = new List<CourseAssociation>();
            List<CourseAssociation> listaTemp = new List<CourseAssociation>();
            
            /* -- Importação Associações UCs UE - INICIO --*/
            SqlConnection conn = new SqlConnection(str);
            listaCourseAssociations = CoursesAssociations(conn, ano, "PTEU01.", "EU.10.QBN");
            /* -- Importação Associações UCs UE - FIM --*/

            /* -- Importação Associações UCs IADE - INICIO --*/
            SqlConnection connIADE = new SqlConnection(strIADE);
            listaTemp = CoursesAssociations(connIADE, ano, "PTEU01.", "IADE");
            foreach(CourseAssociation ca in listaTemp)
            {
                listaCourseAssociations.Add(ca);
            }
            /* -- Importação Associações UCs IADE - FIM --*/

            /* -- Importação Associações UCs IPAM LX - INICIO --*/
            SqlConnection connIPAMLX = new SqlConnection(strIPAMLX);
            listaTemp = CoursesAssociations(connIPAMLX, ano, "PTEU01.", "IPAML");
            foreach (CourseAssociation ca in listaTemp)
            {
                listaCourseAssociations.Add(ca);
            }
            /* -- Importação Associações UCs IPAM LX - FIM --*/

            /* -- Importação Associações UCs IPAM Porto - INICIO --*/
            SqlConnection connIPAMPRT = new SqlConnection(strIPAMPRT);
            listaTemp = CoursesAssociations(connIPAMPRT, ano, "PTEU01.", "IPAMP");
            foreach (CourseAssociation ca in listaTemp)
            {
                listaCourseAssociations.Add(ca);
            }
            /* -- Importação Associações UCs IPAM Porto - FIM --*/

            return listaCourseAssociations;
         }
        #endregion

        #region Courses Associations
        private static List<CourseAssociation> CoursesAssociations(SqlConnection conn, int ano, string EKP, string ENK)
        {
            List<CourseAssociation> listaCourseAssociations = new List<CourseAssociation>();
            string query = "SELECT Distinct cast(Insc_Cp_AnoLect as varchar (8000)) + '_' + cast(Insc_Cx_CdDepartamento as varchar (8000)) + 'C' + cast(Insc_Cx_CdCadeira as varchar (8000)) + '_' + cast(Insc_NHorario as varchar (8000)) as EXTERNAL_COURSE_KEY , '' as DATA_SOURCE_KEY FROM[dbo].[TInscri] WHERE[Insc_Cp_AnoLect] > " + ano + " and Insc_NHorario <> 0";

            SqlCommand cmd = new SqlCommand(query, conn);

            conn.Open();
            SqlDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                CourseAssociation novaAssociacao = new CourseAssociation();
                try { novaAssociacao.EXTERNAL_COURSE_KEY = EKP + Convert.ToString(dr.GetValue(0)); } catch (SqlException ex) {/*TODO write to logs*/ };
                try { novaAssociacao.DATA_SOURCE_KEY = Convert.ToString(dr.GetValue(1)); } catch (SqlException ex) {/*TODO write to logs*/ };
                novaAssociacao.EXTERNAL_NODE_KEY = EKP + ENK;
                listaCourseAssociations.Add(novaAssociacao);
            }
            conn.Close();

            return listaCourseAssociations;
        }
        #endregion



        #region Get Users Associations
        public static List<UsersAssociation> GetUsersAssociations(string str,string strIADE, string strIPAMLX, string strIPAMPRT, int ano)
        {
            List<UsersAssociation> listausersAssociations = new List<UsersAssociation>();
            List<UsersAssociation> listaTemp = new List<UsersAssociation>();

            /* -- Importação Associações Users UE - INICIO --*/
            SqlConnection conn = new SqlConnection(str);
            listausersAssociations = StudentUsersAssociations(conn, ano, "PTEU01.", "EU.10.QBN");
            listaTemp = TeatchersUsersAssociations(conn, ano, "PTEU01.", "EU.10.QBN", "");
            foreach (UsersAssociation ua in listaTemp)
            {
                listausersAssociations.Add(ua);
            }
            /* -- Importação Associações Users UE - FIM --*/

            /* -- Importação Associações Users IADE - INICIO --*/
            SqlConnection connIADE = new SqlConnection(strIADE);
            listaTemp = StudentUsersAssociations(connIADE, ano, "PTEU01.", "IADE");
            foreach(UsersAssociation ua in listaTemp)
            {
                listausersAssociations.Add(ua);
            }
            listaTemp = TeatchersUsersAssociations(connIADE, ano, "PTEU01.", "IADE", "");
            foreach (UsersAssociation ua in listaTemp)
            {
                listausersAssociations.Add(ua);
            }
            /* -- Importação Associações Users IADE - FIM --*/

            /* -- Importação Associações Users IPAM LX - INICIO --*/
            SqlConnection connIPAMLX = new SqlConnection(strIPAMLX);
            listaTemp = StudentUsersAssociations(connIPAMLX, ano, "PTEU01.", "IPAML");
            foreach (UsersAssociation ua in listaTemp)
            {
                listausersAssociations.Add(ua);
            }
            listaTemp = TeatchersUsersAssociations(connIPAMLX, ano, "PTEU01.", "IPAML", "");
            foreach (UsersAssociation ua in listaTemp)
            {
                listausersAssociations.Add(ua);
            }
            /* -- Importação Associações Users IPAM LX - FIM --*/

            /* -- Importação Associações Users IPAM Porto - INICIO --*/
            SqlConnection connIPAMPRT = new SqlConnection(strIPAMPRT);
            listaTemp = StudentUsersAssociations(connIPAMPRT, ano, "PTEU01.", "IPAMP");

            foreach(UsersAssociation ua in listaTemp)
            {
                ua.EXTERNAL_USER_KEY = ua.EXTERNAL_USER_KEY + "@ipam.pt";
            }
            foreach (UsersAssociation ua in listaTemp)
            {
                listausersAssociations.Add(ua);
            }
            listaTemp = TeatchersUsersAssociations(connIPAMPRT, ano, "PTEU01.", "IPAMP", "");
            foreach (UsersAssociation ua in listaTemp)
            {
                listausersAssociations.Add(ua);
            }
            /* -- Importação Associações Users IPAM Porto - FIM --*/

            return listausersAssociations;
        }
        #endregion

        #region Student Users Associations
        private static List<UsersAssociation> StudentUsersAssociations(SqlConnection conn, int ano, string EUK, string EAK)
        {
            List<UsersAssociation> listausersAssociations = new List<UsersAssociation>();

            string query = "select DISTINCT '' as DATA_SOURCE_KEY , cast([Insc_Cx_CdAluno] as varchar (8000)) as EXTERNAL_ASSOCIATION_KEY ,'' as EXTERNAL_NODE_KEY ,cast([Insc_Cx_CdAluno] as varchar (8000)) as EXTERNAL_USER_KEY FROM[dbo].[TInscri] INNER JOIN (TRAluCur INNER JOIN TEntidad ON TRAluCur.RAlc_Ce_CdEntidade = TEntidad.Enti_Cp_CdEntidade) ON[TInscri].[Insc_Cx_CdAluno] = TRAluCur.RAlc_Cp_NAluno where[Insc_Cp_AnoLect] > " + ano ;

            SqlCommand cmd = new SqlCommand(query, conn);
            conn.Open();
            SqlDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                UsersAssociation novaAssociacao = new UsersAssociation();
                try { novaAssociacao.Data_SOURCE_KEY = Convert.ToString(dr.GetValue(0)); } catch (SqlException ex) {/*TODO write to logs*/ };
                try { novaAssociacao.EXTERNAL_ASSOCIATION_KEY = EUK + EAK + "." + Convert.ToString(dr.GetValue(1)); } catch (SqlException ex) {/*TODO write to logs*/ };
                try { novaAssociacao.EXTERNAL_NODE_KEY = EUK + EAK + Convert.ToString(dr.GetValue(2)); } catch (SqlException ex) {/*TODO write to logs*/ };
                try { novaAssociacao.EXTERNAL_USER_KEY = EUK + Convert.ToString(dr.GetValue(3)); } catch (SqlException ex) {/*TODO write to logs*/ };
                listausersAssociations.Add(novaAssociacao);
            }
            conn.Close();

            return listausersAssociations;
        }
        #endregion

        #region Teatchers Users Associations
        private static List<UsersAssociation> TeatchersUsersAssociations(SqlConnection conn, int ano, string EUK, string EAK, string campoTemp)
        {
            List<UsersAssociation> listausersAssociations = new List<UsersAssociation>();

            string query = "select '' as DATA_SOURCE_KEY , cast(Clie_Cp_CdCliTes as varchar (8000)) as EXTERNAL_ASSOCIATION_KEY ,'' as EXTERNAL_NODE_KEY,cast(Clie_Cp_CdCliTes as varchar (8000)) as EXTERNAL_USER_KEY FROM[bdsophis].[dbo].[TCliTes] where[Clie_Ce_TpCliTes] = 5 AND Clie_Estado = 'A'";

            SqlCommand cmd = new SqlCommand(query, conn);
            conn.Open();
            SqlDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                UsersAssociation novaAssociacao = new UsersAssociation();
                try { novaAssociacao.Data_SOURCE_KEY = Convert.ToString(dr.GetValue(0)); } catch (SqlException ex) {/*TODO write to logs*/ };
                try { novaAssociacao.EXTERNAL_ASSOCIATION_KEY = EUK + EAK + "." + campoTemp + Convert.ToString(dr.GetValue(1)); } catch (SqlException ex) {/*TODO write to logs*/ };
                try { novaAssociacao.EXTERNAL_NODE_KEY = EUK + EAK + Convert.ToString(dr.GetValue(2)); } catch (SqlException ex) {/*TODO write to logs*/ };
                try { novaAssociacao.EXTERNAL_USER_KEY = EUK + campoTemp + Convert.ToString(dr.GetValue(3)); } catch (SqlException ex) {/*TODO write to logs*/ };
                listausersAssociations.Add(novaAssociacao);
            }
            conn.Close();

            return listausersAssociations;
        }
        #endregion


        #region Get Sec Inst Role Associations
        public static List<SecInstRoleAssociation> GetSecInstRoleAssociations(string str, string strIADE, string strIPAMLX, string strIPAMPRT, int ano)
        {
            List<SecInstRoleAssociation> listaSecRole = new List<SecInstRoleAssociation>();
            List<SecInstRoleAssociation> listaTemp = new List<SecInstRoleAssociation>();

            /* -- Importação SecInstRole UE - INICIO --*/
            SqlConnection conn = new SqlConnection(str);
            listaSecRole = GetStudentsRole(conn, ano, "PTEU01.");
            listaTemp = GetTeatchersRole(conn, ano, "PTEU01.", "");
            foreach(SecInstRoleAssociation sec in listaTemp)
            {
                listaSecRole.Add(sec);
            }
            /* -- Importação SecInstRole UE - FIM --*/

            /* -- Importação SecInstRole IADE - INICIO --*/
            SqlConnection connIADE = new SqlConnection(strIADE);
            listaTemp = GetStudentsRole(connIADE, ano, "PTEU01.");
            foreach (SecInstRoleAssociation sec in listaTemp)
            {
                listaSecRole.Add(sec);
            }
            listaTemp = GetTeatchersRole(connIADE, ano, "PTEU01.", "");
            foreach (SecInstRoleAssociation sec in listaTemp)
            {
                listaSecRole.Add(sec);
            }
            /* -- Importação SecInstRole IADE - FIM --*/

            /* -- Importação SecInstRole IPAM LX - INICIO --*/
            SqlConnection connIPAMLX = new SqlConnection(strIPAMLX);
            listaTemp = GetStudentsRole(connIPAMLX, ano, "PTEU01.");
            foreach (SecInstRoleAssociation sec in listaTemp)
            {
                listaSecRole.Add(sec);
            }
            listaTemp = GetTeatchersRole(connIPAMLX, ano, "PTEU01.", "");
            foreach (SecInstRoleAssociation sec in listaTemp)
            {
                listaSecRole.Add(sec);
            }
            /* -- Importação SecInstRole IPAM LX - FIM --*/

            /* -- Importação SecInstRole IPAM Porto - INICIO --*/
            SqlConnection connIPAMPRT = new SqlConnection(strIPAMPRT);
            listaTemp = GetStudentsRole(connIPAMPRT, ano, "PTEU01.");
            foreach (SecInstRoleAssociation ua in listaTemp)
            {
                ua.EXTERNAL_PERSON_KEY = ua.EXTERNAL_PERSON_KEY + "@ipam.pt";
            }
            foreach (SecInstRoleAssociation ua in listaTemp)
            {
                listaSecRole.Add(ua);
            }
            listaTemp = GetTeatchersRole(connIPAMPRT, ano, "PTEU01.", "");
            foreach (SecInstRoleAssociation sec in listaTemp)
            {
                listaSecRole.Add(sec);
            }
            /* -- Importação SecInstRole IPAM Porto - FIM --*/

            return listaSecRole;
        }
        #endregion

        #region Get Sec Inst Role Associations Students
        private static List<SecInstRoleAssociation> GetStudentsRole(SqlConnection conn, int ano, string EPK)
        {
            List<SecInstRoleAssociation> listaSecRole = new List<SecInstRoleAssociation>();

            string query = "select DISTINCT '' as DATA_SOURCE_KEY ,'Student' as ROLE_ID , cast([Insc_Cx_CdAluno] as varchar (8000)) as EXTERNAL_PERSON_KEY ,'enabled' as ROW_STATUS FROM[dbo].[TInscri] INNER JOIN (TRAluCur INNER JOIN TEntidad ON TRAluCur.RAlc_Ce_CdEntidade = TEntidad.Enti_Cp_CdEntidade)  ON[TInscri].[Insc_Cx_CdAluno] = TRAluCur.RAlc_Cp_NAluno where[Insc_Cp_AnoLect] > " + ano;

            SqlCommand cmd = new SqlCommand(query, conn);
            conn.Open();
            SqlDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                SecInstRoleAssociation novaAssociacao = new SecInstRoleAssociation();
                try { novaAssociacao.DATA_SOURCE_KEY = Convert.ToString(dr.GetValue(0)); } catch (SqlException ex) {/*TODO write to logs*/ };
                try { novaAssociacao.ROLE_ID = EPK + Convert.ToString(dr.GetValue(1)); } catch (SqlException ex) {/*TODO write to logs*/ };
                try { novaAssociacao.EXTERNAL_PERSON_KEY = EPK + Convert.ToString(dr.GetValue(2)); } catch (SqlException ex) {/*TODO write to logs*/ };
                try { novaAssociacao.ROW_Status = Convert.ToString(dr.GetValue(3)); } catch (SqlException ex) {/*TODO write to logs*/ };
                listaSecRole.Add(novaAssociacao);
            }
            conn.Close();

            return listaSecRole;
        }
        #endregion

        #region Get Sec Inst Role Associations Teatchers
        private static List<SecInstRoleAssociation> GetTeatchersRole(SqlConnection conn, int ano, string EPK, string campoTemp)
        {
            List<SecInstRoleAssociation> listaSecRole = new List<SecInstRoleAssociation>();

            string query = "select '' as DATA_SOURCE_KEY,'Faculty' as ROLE_ID, cast(Clie_Cp_CdCliTes as varchar (8000)) as EXTERNAL_PERSON_KEY,'enabled' as ROW_STATUS FROM[bdsophis].[dbo].[TCliTes] where[Clie_Ce_TpCliTes] = 5 AND Clie_Estado = 'A'";

            SqlCommand cmd = new SqlCommand(query, conn);
            conn.Open();
            SqlDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                SecInstRoleAssociation novaAssociacao = new SecInstRoleAssociation();
                try { novaAssociacao.DATA_SOURCE_KEY = Convert.ToString(dr.GetValue(0)); } catch (SqlException ex) {/*TODO write to logs*/ };
                try { novaAssociacao.ROLE_ID = EPK + Convert.ToString(dr.GetValue(1)); } catch (SqlException ex) {/*TODO write to logs*/ };
                try { novaAssociacao.EXTERNAL_PERSON_KEY = EPK + campoTemp + Convert.ToString(dr.GetValue(2)); } catch (SqlException ex) {/*TODO write to logs*/ };
                try { novaAssociacao.ROW_Status = Convert.ToString(dr.GetValue(3)); } catch (SqlException ex) {/*TODO write to logs*/ };
                listaSecRole.Add(novaAssociacao);
            }
            conn.Close();

            return listaSecRole;
        }
        #endregion



        #region Write Users
        public static void WriteUsers(List<Users> listaUsers)
        {
            Encoding utf8 = new UTF8Encoding(true);
            try
            {
                string header ="EXTERNAL_PERSON_KEY|DATA_SOURCE_KEY|FIRSTNAME|LASTNAME|USER_ID|PASSWD|AVAILABLE_IND|EMAIL|GENDER|MIDDLENAME|INSTITUTION_ROLE|ROW_STATUS|STUDENT_ID|SYSTEM_ROLE" + "\n"; ;
                Byte[] encodedBytes = utf8.GetBytes(header);
                WriteToFile(@"c:\BBLearn\1.Users.csv", utf8, encodedBytes);

                foreach (Users temp in listaUsers)
                {
                    string unicodeString = temp.EXTERNAL_PERSON_KEY + "|" +
                                               temp.DATA_SOURCE_KEY + "|" +
                                                temp.FIRSTNAME + "|" +
                                                temp.LASTNAME + "|" +
                                                temp.USER_ID + "|" +
                                                temp.PASSWD + "|" +
                                                temp.AVAILABLE_IND + "|" +
                                                temp.EMAIL + "|" +
                                                temp.GENDER + "|" +
                                                temp.MIDDLENAME + "|" +
                                                temp.INSTITUTION_ROLE + "|" +
                                                temp.ROW_STATUS + "|" +
                                                temp.STUDENT_ID + "|" +
                                                temp.SYSTEM_ROLE + "\n";

                    encodedBytes = utf8.GetBytes(unicodeString);
                    WriteToFile2(@"c:\BBLearn\1.Users.csv", utf8,encodedBytes);
                }
            }
            catch (IOException ex)
            {
                /*TODO write to logs*/
            }
        }
        #endregion

        #region Write Courses
        public static void WriteCourses(List<Courses> listaCourses)
        {
            Encoding utf8 = new UTF8Encoding(true);
            try
            {
                string header = "COURSE_ID|EXTERNAL_COURSE_KEY|COURSE_NAME|DATA_SOURCE_KEY|DESCRIPTION|DURATION|END_DATE|EXTERNAL_ASSOCIATION_KEY|PRIMARY_EXTERNAL_NODE_KEY|ROW_STATUS|TEMPLATE_COURSE_KEY|START_DATE|ACADEMIC_YEAR" + "\n"; ;
                Byte[] encodedBytes = utf8.GetBytes(header);
                WriteToFile(@"c:\BBLearn\2.Courses.csv", utf8, encodedBytes);

                foreach (Courses temp in listaCourses)
                {
                    string unicodeString = temp.COURSE_ID + "|" +
                            "PTEU01." + temp.COURSE_ID + "|" +
                            temp.COURSE_NAME + "|" +
                            "|" +
                            temp.COURSE_NAME + "|" +
                             "C|" +
                            temp.END_DATE + "|" +
                            "|" +
                            "|" +
                            temp.ROW_SATTUS + "|" +
                            "PTEU01-01" + "|" +
                            temp.START_DATE + "|" +
                            temp.ACADEMIC_YEAR + "\n"; ;
                    encodedBytes = utf8.GetBytes(unicodeString);
                    WriteToFile2(@"c:\BBLearn\2.Courses.csv", utf8, encodedBytes);
                }
            }
            catch (IOException ex)
            {

                /*TODO write to logs*/
            }
        }
        #endregion
        
        #region Write Courses Membership
        public static void WriteCouresesMembership(List<CourseMembership> listaInscricoes)
        {
            Encoding utf8 = new UTF8Encoding(true);
            try
            {
                string header = "EXTERNAL_COURSE_KEY|DATA_SOURCE_KEY|EXTERNAL_PERSON_KEY|AVAILABLE_IND|ROLE|ROW_STATUS|ACADEMIC_YEAR|ID_TITULACION|ID_PRODUCTO" + "\n"; ;
                Byte[] encodedBytes = utf8.GetBytes(header);

                WriteToFile(@"c:\BBLearn\4.Course_Memberships.csv", utf8, encodedBytes);
                foreach (CourseMembership temp in listaInscricoes)
                {
                    string unicodeString = temp.EXTERNAL_COURSE_KEY + "|" +
                                "|" +
                                temp.EXTERNAL_PERSON_KEY + "|" +
                                temp.AVAILABLE_IND + "|" +
                                temp.ROLE + "|" +
                                temp.ROW_STATUS + "|" +
                                temp.ACADEMIC_YEAR + "|" +
                                "|" + "\n"; ;
                    encodedBytes = utf8.GetBytes(unicodeString);
                    WriteToFile2(@"c:\BBLearn\4.Course_Memberships.csv", utf8, encodedBytes);
                }
            }
            catch (IOException ex)
            {
                /*TODO write to logs*/
            }
        }
        #endregion

        #region Write Courses Associations
        public static void WriteCoursesAssociations(List<CourseAssociation> listaCourseAssociation)
        {
            Encoding utf8 = new UTF8Encoding(true);

            string header = "EXTERNAL_COURSE_KEY|DATA_SOURCE_KEY|EXTERNAL_NODE_KEY" + "\n";
            Byte[] encodedBytes = utf8.GetBytes(header);
            WriteToFile(@"c:\BBLearn\13.Hierarchy_Course_Associations.csv", utf8, encodedBytes);

            try
            {
                foreach (CourseAssociation temp in listaCourseAssociation)
                {
                    string unicodeString = temp.EXTERNAL_COURSE_KEY + "|" +
                            temp.DATA_SOURCE_KEY + "|" +
                            temp.EXTERNAL_NODE_KEY + "\n"; ;
                    encodedBytes = utf8.GetBytes(unicodeString);
                    WriteToFile2(@"c:\BBLearn\13.Hierarchy_Course_Associations.csv", utf8, encodedBytes);
                }
             }
            catch (IOException ex)
            {

                /*TODO write to logs*/
            }

        }
        #endregion
        
        #region Write Users Associations
        public static void WriteUsersAssociations(List<UsersAssociation> listaUsersAssociations)
        {
            Encoding utf8 = new UTF8Encoding(true);
            try
            {
                string header = "DATA_SOURCE_KEY|EXTERNAL_ASSOCIATION_KEY|EXTERNAL_NODE_KEY|EXTERNAL_USER_KEY" + "\n"; ;
                Byte[] encodedBytes = utf8.GetBytes(header);
                WriteToFile(@"c:\BBLearn\12.Hierarchy_Users_Associations.csv", utf8, encodedBytes);

                foreach (UsersAssociation temp in listaUsersAssociations)
                {
                    string unicodeString = temp.Data_SOURCE_KEY + "|" +
                            temp.EXTERNAL_ASSOCIATION_KEY + "|" +
                            temp.EXTERNAL_NODE_KEY + "|" +
                            temp.EXTERNAL_USER_KEY + "\n"; ;
                    encodedBytes = utf8.GetBytes(unicodeString);
                    WriteToFile2(@"c:\BBLearn\12.Hierarchy_Users_Associations.csv", utf8, encodedBytes);
                }
            }
            catch (IOException ex)
            {

                /*TODO write to logs*/
            }
        }
        #endregion

        #region Write Sec inst Role Associations
        public static void WriteSecInstRoleAssociations(List<SecInstRoleAssociation> listaSecInstRoleAssociation)
        {
            Encoding utf8 = new UTF8Encoding(true);

            try
            {
                string header = "DATA_SOURCE_KEY|ROLE_ID|EXTERNAL_PERSON_KEY|ROW_STATUS" + "\n"; ;
                Byte[] encodedBytes = utf8.GetBytes(header);
                WriteToFile(@"c:\BBLearn\7.Sec_Inst_Role_Associations.csv", utf8, encodedBytes);

                foreach (SecInstRoleAssociation temp in listaSecInstRoleAssociation)
                {
                    string unicodeString = temp.DATA_SOURCE_KEY + "|" +
                            temp.ROLE_ID + "|" +
                            temp.EXTERNAL_PERSON_KEY + "|" +
                            temp.ROW_Status + "\n"; ;
                    encodedBytes = utf8.GetBytes(unicodeString);
                    WriteToFile2(@"c:\BBLearn\7.Sec_Inst_Role_Associations.csv", utf8, encodedBytes);
                }
            }
            catch (IOException ex)
            {

                /*TODO write to logs*/
            }
        }
        #endregion
        
        #region Write Categories
        private static void WriteCategories()
        {
            Encoding utf8 = new UTF8Encoding(true);
            string linha1 = "DATA_SOURCE_KEY|TITLE|EXTERNAL_CATEGORY_KEY|DESCRIPTION|AVAILABLE_IND|PARENT_CATEGORY_KEY|ROW_STATUS" + "\n"; ;
            Byte[] encodedBytes = utf8.GetBytes(linha1);
            WriteToFile(@"c:\BBLearn\9.Categories.csv", utf8, encodedBytes);

            string linha2 = "|Universidade Europeia|PTEU01|Universidade Europeia - Portugal|Y|PTEU01|enabled" + "\n"; 
            encodedBytes = utf8.GetBytes(linha2);
            WriteToFile2(@"c:\BBLearn\9.Categories.csv", utf8, encodedBytes);

            string linha3 = "|IADE|PTEU01|IADE - Portugal|Y|PTEU01|enabled" + "\n"; 
            encodedBytes = utf8.GetBytes(linha3);
            WriteToFile2(@"c:\BBLearn\9.Categories.csv", utf8, encodedBytes);

            string linha4 = "|IPAM Lisboa|PTEU01|IPAM Lisboa - Portugal|Y|PTEU01|enabled" + "\n";
            encodedBytes = utf8.GetBytes(linha4);
            WriteToFile2(@"c:\BBLearn\9.Categories.csv", utf8, encodedBytes);

            string linha5 = "|IPAM Porto|PTEU01|IPAM Porto - Portugal|Y|PTEU01|enabled" + "\n";
            encodedBytes = utf8.GetBytes(linha5);
            WriteToFile2(@"c:\BBLearn\9.Categories.csv", utf8, encodedBytes);
        }
        #endregion

        #region Write Hierarchy Nodes
        private static void WritehierarchyNodes()
        {
            Encoding utf8 = new UTF8Encoding(true);
            string linha1 = "EXTERNAL_NODE_KEY|DATA_SOURCE_KEY|PARENT_NODE_KEY|NAME" + "\n";
            Byte[] encodedBytes = utf8.GetBytes(linha1);
            WriteToFile(@"c:\BBLearn\11.Hierarchy_Nodes.csv", utf8, encodedBytes);

            string linha2 = "PTEU01.EU||PTEU01|Universidade Europeia - PTEU01" + "\n";
            encodedBytes = utf8.GetBytes(linha2);
            WriteToFile2(@"c:\BBLearn\11.Hierarchy_Nodes.csv", utf8, encodedBytes);

            string linha3 = "PTEU01.IADE||PTEU01|IADE" + "\n"; ;
            encodedBytes = utf8.GetBytes(linha3);
            WriteToFile2(@"c:\BBLearn\11.Hierarchy_Nodes.csv", utf8, encodedBytes);

            string linha4 = "PTEU01.IPAML||PTEU01|IPAM Lisboa" + "\n"; ;
            encodedBytes = utf8.GetBytes(linha4);
            WriteToFile2(@"c:\BBLearn\11.Hierarchy_Nodes.csv", utf8, encodedBytes);

            string linha5 = "PTEU01.IPAMP||PTEU01|IPAM Porto" + "\n"; ;
            encodedBytes = utf8.GetBytes(linha5);
            WriteToFile2(@"c:\BBLearn\11.Hierarchy_Nodes.csv", utf8, encodedBytes);

            string linha6 = "PTEU01.EU.10.QBN||PTEU01|Campus Quinta do Bom Nome" + "\n"; 
            encodedBytes = utf8.GetBytes(linha6);
            WriteToFile2(@"c:\BBLearn\11.Hierarchy_Nodes.csv", utf8, encodedBytes);
        }
        #endregion

        #region Write Organizations
        public static void WriteOrganizations()
        {
            Encoding utf8 = new UTF8Encoding(true);
            string linha1 = "EXTERNAL_ORGANIZATION_KEY|ORGANIZATION_ID|ORGANIZATION_NAME|DESCRIPTION|DURATION|END_DATE|EXTERNAL_ASSOCIATION_KEY|PRIMARY_EXTERNAL_NODE_KEY|ROW_STATUS|TEMPLATE_ORGANIZATION_KEY|START_DATE|TERM_KEY|ACADEMIC_YEAR|ALLOW_GUEST_IND|ALLOW_OBSERVER_IND" + "\n";
            Byte[] encodedBytes = utf8.GetBytes(linha1);
            WriteToFile(@"c:\BBLearn\3.Organizations.csv", utf8, encodedBytes);

            string linha2 = "PTEU01.PTEU01-003|PTEU01-003|Estudantes Universidade Europeia|PTEU01-003|C||PTEU01.EU|PTEU01.EU.10.QBN|enabled|PTEU01-01||||N|N" + "\n";
            encodedBytes = utf8.GetBytes(linha2);
            WriteToFile2(@"c:\BBLearn\3.Organizations.csv", utf8, encodedBytes);

            string linha3 = "PTEU01.PTEU01-002|PTEU01-002|Docentes Universidade Europeia|PTEU01-002|C||PTEU01.EU|PTEU01.EU.10.QBN|enabled|PTEU01-01||||N|N" + "\n";
            encodedBytes = utf8.GetBytes(linha3);
            WriteToFile2(@"c:\BBLearn\3.Organizations.csv", utf8, encodedBytes);

            string linha4 = "PTEU01.PTIA01-002|PTIA01-002|Estudantes IADE|PTIA01-002|C||PTEU01.IADE|PTEU01.IADE|enabled|PTEU01-01||||N|N" + "\n";
            encodedBytes = utf8.GetBytes(linha4);
            WriteToFile2(@"c:\BBLearn\3.Organizations.csv", utf8, encodedBytes);

            string linha5 = "PTEU01.PTIA01-001|PTIA01-001|Docentes IADE|PTIA01-001|C||PTEU01.IADE|PTEU01.IADE|enabled|PTEU01-01||||N|N" + "\n";
            encodedBytes = utf8.GetBytes(linha5);
            WriteToFile2(@"c:\BBLearn\3.Organizations.csv", utf8, encodedBytes);

            string linha6 = "PTEU01.PTIP01-002|PTIP01-002|Estudantes IPAM Porto|PTIP01-002|C||PTEU01.IPAMP|PTEU01.IPAMP|enabled|PTEU01-01||||N|N" + "\n";
            encodedBytes = utf8.GetBytes(linha6);
            WriteToFile2(@"c:\BBLearn\3.Organizations.csv", utf8, encodedBytes);

            string linha7 = "PTEU01.PTIP01-001|PTIP01-001|Docentes IPAM Porto|PTIP01-001|C||PTEU01.IPAMP|PTEU01.IPAMP|enabled|PTEU01-01||||N|N" + "\n";
            encodedBytes = utf8.GetBytes(linha7);
            WriteToFile2(@"c:\BBLearn\3.Organizations.csv", utf8, encodedBytes);

            string linha8 = "PTEU01.PTIP01-004|PTIP01-004|Estudantes IPAM Lisboa|PTIP01-004|C||PTEU01.IPAML|PTEU01.IPAML|enabled|PTEU01-01||||N|N" + "\n";
            encodedBytes = utf8.GetBytes(linha8);
            WriteToFile2(@"c:\BBLearn\3.Organizations.csv", utf8, encodedBytes);

            string linha9 = "PTEU01.PTIP01-003|PTIP01-003|Docentes IPAM Lisboa|PTIP01-003|C||PTEU01.IPAML|PTEU01.IPAML|enabled|PTEU01-01||||N|N" + "\n";
            encodedBytes = utf8.GetBytes(linha9);
            WriteToFile2(@"c:\BBLearn\3.Organizations.csv", utf8, encodedBytes);

            string linha10 = "PTEU01.PTEU01-008|PTEU01-008|≠ Academia de Competências|PTEU01-008|C||PTEU01.EU|PTEU01.EU.10.QBN|enabled|PTEU01-01||||N|N" + "\n";
            encodedBytes = utf8.GetBytes(linha10);
            WriteToFile2(@"c:\BBLearn\3.Organizations.csv", utf8, encodedBytes);

            string linha11 = "PTEU01.PTEU01-009|PTEU01-009|Biblioteca|PTEU01-009|C||PTEU01.EU|PTEU01.EU.10.QBN|enabled|PTEU01-01||||N|N" + "\n";
            encodedBytes = utf8.GetBytes(linha11);
            WriteToFile2(@"c:\BBLearn\3.Organizations.csv", utf8, encodedBytes);

            //string linha12 = "PTEU01.PTEU01-010|PTEU01-010|Investigação IPAM|PTEU01-010|C||PTEU01.EU|PTEU01.IPAML|enabled|PTEU01-01||||N|N" + "\n";
            //encodedBytes = utf8.GetBytes(linha12);
            //WriteToFile2(@"c:\BBLearn\3.Organizations.csv", utf8, encodedBytes);

            string linha13 = "PTEU01.PTEU01-012|PTEU01-012|Vida Académica|PTEU01-012|C||PTEU01.EU|PTEU01.EU.10.QBN|enabled|PTEU01-01||||N|N" + "\n";
            encodedBytes = utf8.GetBytes(linha13);
            WriteToFile2(@"c:\BBLearn\3.Organizations.csv", utf8, encodedBytes);

            string linha14 = "PTEU01.PTEU01-022|PTEU01-022|Empregabilidade|PTEU01-022|C||PTEU01.EU|PTEU01.EU.10.QBN|enabled|PTEU01-01||||N|N" + "\n";
            encodedBytes = utf8.GetBytes(linha14);
            WriteToFile2(@"c:\BBLearn\3.Organizations.csv", utf8, encodedBytes);

            string linha15 = "PTEU01.PTEU01-013|PTEU01-013|Recursos IT|PTEU01-013|C||PTEU01.EU|PTEU01.EU.10.QBN|enabled|PTEU01-01||||N|N" + "\n";
            encodedBytes = utf8.GetBytes(linha15);
            WriteToFile2(@"c:\BBLearn\3.Organizations.csv", utf8, encodedBytes);
        }
        #endregion

        #region Write Organizations Membership
        private static void writeOrganizationsMembership(List<OrganizationMembership> listaOrganizationsMembership)
        {
            Encoding utf8 = new UTF8Encoding(true);
            try
            {
                string header = "EXTERNAL_ORGANIZATION_KEY|DATA_SOURCE_KEY |EXTERNAL_PERSON_KEY|AVAILABLE_IND|ROLE|ROW_STATUS|ACADEMIC_YEAR" + "\n";
                Byte[] encodedBytes = utf8.GetBytes(header);
                WriteToFile(@"c:\BBLearn\5.Organization_Memberships.csv", utf8, encodedBytes);
                foreach(OrganizationMembership temp in listaOrganizationsMembership)
                {
                    string unicodeString = temp.EXTERNAL_ORGANIZATION_KEY + "|" +
                                           temp.DATA_SOURCE_KEY + "|" +
                                           temp.EXTERNAL_PERSON_KEY + "|" +
                                           temp.AVAILABLE_IND + "|" +
                                           temp.ROLE + "|" +
                                           temp.ROW_STATUS + "|" +
                                           temp.ACADEMIC_YEAR + "\n";
                    encodedBytes = utf8.GetBytes(unicodeString);
                    WriteToFile2(@"C:\BBlearn\5.Organization_Memberships.csv", utf8, encodedBytes);
                }
            }
            catch (IOException ex)
            {
                /*TODO write to logs*/
            }
        }
        #endregion


        private static void WriteToFile(String fn, Encoding enc, Byte[] bytes)
        {
            var fs = new FileStream(fn, FileMode.Append);
            Byte[] preamble = enc.GetPreamble();
            fs.Write(preamble, 0, preamble.Length);
            fs.Write(bytes, 0, bytes.Length);
            fs.Close();
        }
        private static void WriteToFile2(String fn, Encoding enc, Byte[] bytes)
        {
            var fs = new FileStream(fn, FileMode.Append);
            fs.Write(bytes, 0, bytes.Length);
            fs.Close();
        }

        private static void CreateFiles()
        {
            var fs = new FileStream(@"C:\BBlearn\1.Users.csv", FileMode.Create);
            fs.Close();

            fs = new FileStream(@"C:\BBlearn\2.Courses.csv", FileMode.Create);
            fs.Close();

            fs = new FileStream(@"C:\BBlearn\3.Organizations.csv", FileMode.Create);
            fs.Close();

            fs = new FileStream(@"C:\BBlearn\4.Course_Memberships.csv", FileMode.Create);
            fs.Close();

            fs = new FileStream(@"C:\BBlearn\5.Organization_Memberships.csv", FileMode.Create);
            fs.Close();

            fs = new FileStream(@"C:\BBlearn\7.Sec_Inst_Role_Associations.csv", FileMode.Create);
            fs.Close();

            fs = new FileStream(@"C:\BBlearn\9.Categories.csv", FileMode.Create);
            fs.Close();

            fs = new FileStream(@"C:\BBlearn\11.Hierarchy_Nodes.csv", FileMode.Create);
            fs.Close();

            fs = new FileStream(@"C:\BBlearn\12.Hierarchy_Users_Associations.csv", FileMode.Create);
            fs.Close();

            fs = new FileStream(@"C:\BBlearn\13.Hierarchy_Course_Associations.csv", FileMode.Create);
            fs.Close();
        }
    }
}
