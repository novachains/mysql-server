#include <my_sys.h>
#include <mysql.h>
C_MODE_START;
my_bool getdigest_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void getdigest_deinit(UDF_INIT *initid);
const char *getdigest(UDF_INIT *initid, UDF_ARGS *args, char *result,
           unsigned long *length, char *is_null, char *error);

my_bool verifydigest_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void verifydigest_deinit(UDF_INIT *initid);
const char *verifydigest(UDF_INIT *initid, UDF_ARGS *args, char *result,
           unsigned long *length, char *is_null, char *error);

my_bool getentry_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void getentry_deinit(UDF_INIT *initid);
const char *getentry(UDF_INIT *initid, UDF_ARGS *args, char *result,
           unsigned long *length, char *is_null, char *error);

my_bool verifyentry_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void verifyentry_deinit(UDF_INIT *initid);
const char *verifyentry(UDF_INIT *initid, UDF_ARGS *args, char *result,
           unsigned long *length, char *is_null, char *error);
C_MODE_END;
