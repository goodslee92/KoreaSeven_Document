/*
 * k7Delivery.h
 */

#define NARRAY              10      /* array processing */
#define TOOMANY             -2112   /* selected rows are too many */
#define SQLNOTFOUND         1403
#define SQL_OK              0
#define SQL_DUPLICATE       -1
#define SQLDISCONNECT_0     -3113
#define SQLDISCONNECT_1     -3114

#define RET_OK          0
#define RET_NG          -1
#define RET_NG2         -2
#define RET_NG3         -3
#define RET_DUP         1

#define SQLCODE             sqlca.sqlcode
#define SQLCNT              sqlca.sqlerrd[2]
#define SQLERRTXT           (sqlca.sqlerrm.sqlerrmc)

#define MAX_IRT_SIZE        20000

sql_context Ctx;


typedef struct {
    char MesgCd      [ 4];  /* 1000 */
    char RespCd      [ 4];  /* 응답코드 (요청시:00) */
    char TranDt      [14];  /* 전송일시 */
    char ReqCnt      [ 3];  /* 전송시간 */
} stDeliHdr;
#define stDeliHdrSize sizeof(stDeliHdr)

typedef struct {
    char MesgCd      [ 4+1];
    char RespCd      [ 4+1];
    char TranDt      [14+1];
    char ReqCnt      [ 3+1];
} stDeliHdrSQL;
#define stDeliHdrSQLSize sizeof(stDeliHdrSQL)

typedef struct
{
    char Gbn                [  2];
    char ReceiptNo          [ 30];
    char Shipper            [224];
    char Consignee          [ 50];
    char ConsTelNo          [ 20];
    char ConsMobPhone       [ 20];
    char ConsPostNo         [ 10];
    char ConsAddr1          [100];
    char ConsAddr2          [100];
    char ItemNm             [200];
    char Amount1            [  9];
    char Amount2            [  9];
    char Amount3            [  9];
    char ReceiptDy          [  8];
    char Filler             [100];
} stDeliData;
#define stDeliDataSize      sizeof(stDeliData)

typedef struct
{
    char Gbn                [  2+1];
    char ReceiptNo          [ 30+1];
    char Shipper            [224+1];
    char Consignee          [ 50+1];
    char ConsTelNo          [ 20+1];
    char ConsMobPhone       [ 20+1];
    char ConsPostNo         [ 10+1];
    char ConsAddr1          [100+1];
    char ConsAddr2          [100+1];
    char ItemNm             [200+1];
    char Amount1            [  9+1];
    char Amount2            [  9+1];
    char Amount3            [  9+1];
    char ReceiptDy          [  8+1];
    char Filler             [100+1];
} stDeliDataSQL;
#define stDeliDataSQLSize         sizeof(stDeliDataSQL)


/* 응답 수신 *************************************************/
typedef struct {
    char ReceiptNo   [ 30];   /* 거래관리번호 */
    char RespCd      [  4];   /* 응답코드 */
    char ErrorMesg   [ 50];
    char Filler      [100];
} stDeliRcv;
#define stDeliRcvSize sizeof(stDeliRcv)

/* 응답 수신 SQL */
typedef struct {
    char ReceiptNo  [ 30+1];
    char RespCd     [  4+1];
    char ErrorMesg  [ 50+1];
    char Filler     [100+1];
} stDeliRcvSQL;
#define stDeliRcvSQLSize sizeof(stDeliRcvSQL)

/*---------------------------------------------------------------------------*/

/* store thread table structure (점포 송수신용 쓰레드 테이블) ****************/
#ifdef  __UNIX_THREAD
typedef struct {
    pthread_t hThread;                              /*thread handle*/
    pthread_cond_t hEvent;                          /*event handle*/
    pthread_mutex_t hMutex;                         /*mutex handle*/
    int nSockFlag;                                  /*socket flag*/
    int nSocket;                                    /*socket id*/
    char CommHeadBuf[stDeliHdrSize+1];  /*CommHead Buffer*/
    char ClientName[16];                            /*Client Name*/
} stStoreThreadTable;
#define stStoreThreadTableSize sizeof(stStoreThreadTable)
#endif

void GetCompanyName(char *ChannelCode, char *CompanyName);
int K7_InitHostSocket(int *nSocket);
int K7_HostSendProc(int *nSocket, char *lpszBuf);
int K7_HostRecvProc(int *nSocket, char *lpszBuf, int nLen);
int proc_iconv(char *src_msg, char *src_type, char *dest_msg, char *dest_type, int dest_size);
void PsLeftTrimStr(char *str);


/******************************************************************************
 *  Source Name   :  k7Delivery.h
******************************************************************************/
