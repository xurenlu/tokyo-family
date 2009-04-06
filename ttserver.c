/*************************************************************************************************
 * The server of Tokyo Tyrant
 *                                                      Copyright (C) 2006-2009 Mikio Hirabayashi
 * This file is part of Tokyo Tyrant.
 * Tokyo Tyrant is free software; you can redistribute it and/or modify it under the terms of
 * the GNU Lesser General Public License as published by the Free Software Foundation; either
 * version 2.1 of the License or any later version.  Tokyo Tyrant is distributed in the hope
 * that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 * You should have received a copy of the GNU Lesser General Public License along with Tokyo
 * Tyrant; if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330,
 * Boston, MA 02111-1307 USA.
 *************************************************************************************************/


#include <ttutil.h>
#include <tculog.h>
#include <tcrdb.h>
#include "myconf.h"
#include "scrext.h"

#define DEFPORT        1978              // default port
#define DEFTHNUM       8                 // default thread number
#define DEFPIDPATH     "ttserver.pid"    // default name of the PID file
#define DEFRTSPATH     "ttserver.rts"    // default name of the RTS file
#define MAXARGSIZ      (32*1024*1024)    // maximum size of each argument
#define MAXARGNUM      (1*1024*1024)     // maximum number of arguments
#define NUMBUFSIZ      32                // size of a numeric buffer
#define LINEBUFSIZ     8192              // size of a line buffer
#define TOKENUNIT      256               // unit number of tokens
#define RECMTXNUM      31                // number of mutexes of records
#define STASHBNUM      1021              // bucket number of the script stash object

#define TTMSKPUT       (1ULL<<0)         /* bit mask of put command */
#define TTMSKPUTKEEP   (1ULL<<1)         /* bit mask of putkeep command */
#define TTMSKPUTCAT    (1ULL<<2)         /* bit mask of putcat command */
#define TTMSKPUTSHL    (1ULL<<3)         /* bit mask of putshl command */
#define TTMSKPUTNR     (1ULL<<4)         /* bit mask of putnr command */
#define TTMSKOUT       (1ULL<<5)         /* bit mask of out command */
#define TTMSKGET       (1ULL<<6)         /* bit mask of get command */
#define TTMSKMGET      (1ULL<<7)         /* bit mask of mget command */
#define TTMSKVSIZ      (1ULL<<8)         /* bit mask of vsiz command */
#define TTMSKITERINIT  (1ULL<<9)         /* bit mask of iterinit command */
#define TTMSKITERNEXT  (1ULL<<10)        /* bit mask of iternext command */
#define TTMSKFWMKEYS   (1ULL<<11)        /* bit mask of fwmkeys command */
#define TTMSKADDINT    (1ULL<<12)        /* bit mask of addint command */
#define TTMSKADDDOUBLE (1ULL<<13)        /* bit mask of adddouble command */
#define TTMSKEXT       (1ULL<<14)        /* bit mask of ext command */
#define TTMSKSYNC      (1ULL<<15)        /* bit mask of sync command */
#define TTMSKVANISH    (1ULL<<16)        /* bit mask of vanish command */
#define TTMSKCOPY      (1ULL<<17)        /* bit mask of copy command */
#define TTMSKRESTORE   (1ULL<<18)        /* bit mask of restore command */
#define TTMSKSETMST    (1ULL<<19)        /* bit mask of setmst command */
#define TTMSKRNUM      (1ULL<<20)        /* bit mask of rnum command */
#define TTMSKSIZE      (1ULL<<21)        /* bit mask of size command */
#define TTMSKSTAT      (1ULL<<22)        /* bit mask of stat command */
#define TTMSKMISC      (1ULL<<23)        /* bit mask of stat command */
#define TTMSKREPL      (1ULL<<24)        /* bit mask of repl command */
#define TTMSKSLAVE     (1ULL<<25)        /* bit mask of slave command */
#define TTMSKALLORG    (1ULL<<26)        /* bit mask of all commands the original */
#define TTMSKALLMC     (1ULL<<27)        /* bit mask of all commands the memcached */
#define TTMSKALLHTTP   (1ULL<<28)        /* bit mask of all commands the HTTP */
#define TTMSKALLREAD   (1ULL<<29)        /* bit mask of all commands of reading */
#define TTMSKALLWRITE  (1ULL<<30)        /* bit mask of all commands of writing */
#define TTMSKALLMANAGE (1ULL<<31)        /* bit mask of all commands of managing */

typedef struct {                         // type of structure of logging opaque object
  int fd;
} LOGARG;

typedef struct {                         // type of structure of master synchronous object
  char host[TTADDRBUFSIZ];
  int port;
  const char *rtspath;
  uint64_t rts;
  TCADB *adb;
  TCULOG *ulog;
  uint32_t sid;
  bool fail;
  bool recon;
} REPLARG;

typedef struct {                         // type of structure of periodic command
  const char *name;
  TCADB *adb;
  TCULOG *ulog;
  uint32_t sid;
  REPLARG *sarg;
  void *scrext;
} EXTPCARG;

typedef struct {                         // type of structure of task opaque object
  uint64_t mask;
  TCADB *adb;
  TCULOG *ulog;
  uint32_t sid;
  REPLARG *sarg;
  pthread_mutex_t rmtxs[RECMTXNUM];
  void **screxts;
} TASKARG;


/* global variables */
const char *g_progname = NULL;           // program name
double g_starttime = 0.0;                // start time
TTSERV *g_serv = NULL;                   // server object
int g_loglevel = TTLOGINFO;              // whether to log debug information
bool g_restart = false;                  // restart flag


/* function prototypes */
int main(int argc, char **argv);
static void usage(void);
static uint64_t getcmdmask(const char *expr);
static void sigtermhandler(int signum);
static void sigchldhandler(int signum);
static int proc(const char *dbname, const char *host, int port, int thnum, int tout,
                bool dmn, const char *pidpath, bool kl, const char *logpath,
                const char *ulogpath, uint64_t ulim, bool uas, uint32_t sid,
                const char *mhost, int mport, const char *rtspath, const char *extpath,
                const TCLIST *extpcs, uint64_t mask);
static void do_log(int level, const char *msg, void *opq);
static void do_slave(void *opq);
static void do_extpc(void *opq);
static void do_task(TTSOCK *sock, void *opq, TTREQ *req);
static char **tokenize(char *str, int *np);
static uint32_t recmtxidx(const char *kbuf, int ksiz);
static void do_put(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_putkeep(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_putcat(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_putshl(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_putnr(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_out(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_get(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_mget(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_vsiz(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_iterinit(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_iternext(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_fwmkeys(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_addint(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_adddouble(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_ext(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_sync(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_vanish(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_copy(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_restore(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_setmst(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_rnum(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_size(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_stat(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_misc(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_repl(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_mc_set(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_add(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_replace(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_get(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_delete(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_incr(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_decr(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_stats(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_flushall(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_version(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_quit(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_http_get(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri);
static void do_http_head(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri);
static void do_http_put(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri);
static void do_http_post(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri);
static void do_http_delete(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri);


/* main routine */
int main(int argc, char **argv){
  g_progname = argv[0];
  g_starttime = tctime();
  char *dbname = NULL;
  char *host = NULL;
  char *pidpath = NULL;
  char *logpath = NULL;
  char *ulogpath = NULL;
  char *mhost = NULL;
  char *rtspath = NULL;
  char *extpath = NULL;
  TCLIST *extpcs = NULL;
  int port = DEFPORT;
  int thnum = DEFTHNUM;
  int tout = 0;
  bool dmn = false;
  bool kl = false;
  uint64_t ulim = 0;
  bool uas = false;
  uint32_t sid = 0;
  int mport = DEFPORT;
  uint64_t mask = 0;
  for(int i = 1; i < argc; i++){
    if(!dbname && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-host")){
        if(++i >= argc) usage();
        host = argv[i];
      } else if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-thnum")){
        if(++i >= argc) usage();
        thnum = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-tout")){
        if(++i >= argc) usage();
        tout = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-dmn")){
        dmn = true;
      } else if(!strcmp(argv[i], "-pid")){
        if(++i >= argc) usage();
        pidpath = argv[i];
      } else if(!strcmp(argv[i], "-kl")){
        kl = true;
      } else if(!strcmp(argv[i], "-log")){
        if(++i >= argc) usage();
        logpath = argv[i];
      } else if(!strcmp(argv[i], "-ld")){
        g_loglevel = TTLOGDEBUG;
      } else if(!strcmp(argv[i], "-le")){
        g_loglevel = TTLOGERROR;
      } else if(!strcmp(argv[i], "-ulog")){
        if(++i >= argc) usage();
        ulogpath = argv[i];
      } else if(!strcmp(argv[i], "-ulim")){
        if(++i >= argc) usage();
        ulim = tcatoix(argv[i]);
      } else if(!strcmp(argv[i], "-uas")){
        uas = true;
      } else if(!strcmp(argv[i], "-sid")){
        if(++i >= argc) usage();
        sid = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-mhost")){
        if(++i >= argc) usage();
        mhost = argv[i];
      } else if(!strcmp(argv[i], "-mport")){
        if(++i >= argc) usage();
        mport = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-rts")){
        if(++i >= argc) usage();
        rtspath = argv[i];
      } else if(!strcmp(argv[i], "-ext")){
        if(++i >= argc) usage();
        extpath = argv[i];
      } else if(!strcmp(argv[i], "-extpc")){
        if(!extpcs) extpcs = tclistnew2(1);
        if(++i >= argc) usage();
        tclistpush2(extpcs, argv[i]);
        if(++i >= argc) usage();
        tclistpush2(extpcs, argv[i]);
      } else if(!strcmp(argv[i], "-mask")){
        if(++i >= argc) usage();
        mask |= getcmdmask(argv[i]);
      } else if(!strcmp(argv[i], "-unmask")){
        if(++i >= argc) usage();
        mask &= ~getcmdmask(argv[i]);
      } else if(!strcmp(argv[i], "--version")){
        printf("Tokyo Tyrant version %s (%d:%s) for %s\n",
               ttversion, _TT_LIBVER, _TT_PROTVER, TTSYSNAME);
        printf("Copyright (C) 2006-2009 Mikio Hirabayashi\n");
        exit(0);
      } else {
        usage();
      }
    } else if(!dbname){
      dbname = argv[i];
    } else {
      usage();
    }
  }
  if(!dbname) dbname = "*";
  if(thnum < 1 || mport < 1) usage();
  if(dmn && !pidpath) pidpath = DEFPIDPATH;
  if(sid < 1){
    sid = port;
    char name[TTADDRBUFSIZ];
    if(ttgetlocalhostname(name)){
      for(int i = 0; name[i] != '\0'; i++){
        sid = sid * 31 + ((unsigned char *)name)[i];
      }
    }
    sid = sid & INT_MAX;
  }
  if(!rtspath) rtspath = DEFRTSPATH;
  g_serv = ttservnew();
  int rv = proc(dbname, host, port, thnum, tout, dmn, pidpath, kl, logpath,
                ulogpath, ulim, uas, sid, mhost, mport, rtspath, extpath, extpcs, mask);
  ttservdel(g_serv);
  if(extpcs) tclistdel(extpcs);
  return rv;
}


/* print the usage and exit */
static void usage(void){
  fprintf(stderr, "%s: the server of Tokyo Tyrant\n", g_progname);
  fprintf(stderr, "\n");
  fprintf(stderr, "usage:\n");
  fprintf(stderr, "  %s [-host name] [-port num] [-thnum num] [-tout num]"
          " [-dmn] [-pid path] [-kl] [-log path] [-ld|-le] [-ulog path] [-ulim num] [-uas]"
          " [-sid num] [-mhost name] [-mport num] [-rts path] [-ext path] [-extpc name period]"
          " [-mask expr] [-unmask expr] [dbname]\n", g_progname);
  fprintf(stderr, "\n");
  exit(1);
}


/* get the bit mask of a command name */
static uint64_t getcmdmask(const char *expr){
  uint64_t mask = 0;
  TCLIST *fields = tcstrsplit(expr, " ,");
  for(int i = 0; i < tclistnum(fields); i++){
    const char *name = tclistval2(fields, i);
    if(tcstrifwm(name, "0x")){
      mask |= strtoll(name, NULL, 16);
    } else if(!tcstricmp(name, "put")){
      mask |= TTMSKPUT;
    } else if(!tcstricmp(name, "putkeep")){
      mask |= TTMSKPUTKEEP;
    } else if(!tcstricmp(name, "putcat")){
      mask |= TTMSKPUTCAT;
    } else if(!tcstricmp(name, "putshl")){
      mask |= TTMSKPUTSHL;
    } else if(!tcstricmp(name, "putnr")){
      mask |= TTMSKPUTNR;
    } else if(!tcstricmp(name, "out")){
      mask |= TTMSKOUT;
    } else if(!tcstricmp(name, "get")){
      mask |= TTMSKGET;
    } else if(!tcstricmp(name, "mget")){
      mask |= TTMSKMGET;
    } else if(!tcstricmp(name, "vsiz")){
      mask |= TTMSKVSIZ;
    } else if(!tcstricmp(name, "iterinit")){
      mask |= TTMSKITERINIT;
    } else if(!tcstricmp(name, "iternext")){
      mask |= TTMSKITERNEXT;
    } else if(!tcstricmp(name, "fwmkeys")){
      mask |= TTMSKFWMKEYS;
    } else if(!tcstricmp(name, "addint")){
      mask |= TTMSKADDINT;
    } else if(!tcstricmp(name, "adddouble")){
      mask |= TTMSKADDDOUBLE;
    } else if(!tcstricmp(name, "ext")){
      mask |= TTMSKEXT;
    } else if(!tcstricmp(name, "sync")){
      mask |= TTMSKSYNC;
    } else if(!tcstricmp(name, "vanish")){
      mask |= TTMSKVANISH;
    } else if(!tcstricmp(name, "copy")){
      mask |= TTMSKCOPY;
    } else if(!tcstricmp(name, "restore")){
      mask |= TTMSKRESTORE;
    } else if(!tcstricmp(name, "setmst")){
      mask |= TTMSKSETMST;
    } else if(!tcstricmp(name, "rnum")){
      mask |= TTMSKRNUM;
    } else if(!tcstricmp(name, "size")){
      mask |= TTMSKSIZE;
    } else if(!tcstricmp(name, "stat")){
      mask |= TTMSKSTAT;
    } else if(!tcstricmp(name, "misc")){
      mask |= TTMSKMISC;
    } else if(!tcstricmp(name, "repl")){
      mask |= TTMSKREPL;
    } else if(!tcstricmp(name, "slave")){
      mask |= TTMSKSLAVE;
    } else if(!tcstricmp(name, "all")){
      mask |= UINT64_MAX;
    } else if(!tcstricmp(name, "allorg")){
      mask |= TTMSKALLORG;
    } else if(!tcstricmp(name, "allmc")){
      mask |= TTMSKALLMC;
    } else if(!tcstricmp(name, "allhttp")){
      mask |= TTMSKALLHTTP;
    } else if(!tcstricmp(name, "allread")){
      mask |= TTMSKALLREAD;
    } else if(!tcstricmp(name, "allwrite")){
      mask |= TTMSKALLWRITE;
    } else if(!tcstricmp(name, "allmanage")){
      mask |= TTMSKALLMANAGE;
    }
  }
  tclistdel(fields);
  return mask;
}


/* handle termination signals */
static void sigtermhandler(int signum){
  if(signum == SIGHUP) g_restart = true;
  ttservkill(g_serv);
}


/* handle child event signals */
static void sigchldhandler(int signum){
  return;
}


/* perform the command */
static int proc(const char *dbname, const char *host, int port, int thnum, int tout,
                bool dmn, const char *pidpath, bool kl, const char *logpath,
                const char *ulogpath, uint64_t ulim, bool uas, uint32_t sid,
                const char *mhost, int mport, const char *rtspath, const char *extpath,
                const TCLIST *extpcs, uint64_t mask){
  LOGARG larg;
  larg.fd = 1;
  ttservsetloghandler(g_serv, do_log, &larg);
  if(dmn){
    if(dbname && *dbname != '*' && *dbname != '+' && *dbname != MYPATHCHR)
      ttservlog(g_serv, TTLOGINFO, "warning: dbname(%s) is not the absolute path", dbname);
    if(port == 0 && host && *host != MYPATHCHR)
      ttservlog(g_serv, TTLOGINFO, "warning: host(%s) is not the absolute path", host);
    if(pidpath && *pidpath != MYPATHCHR)
      ttservlog(g_serv, TTLOGINFO, "warning: pid(%s) is not the absolute path", pidpath);
    if(logpath && *logpath != MYPATHCHR)
      ttservlog(g_serv, TTLOGINFO, "warning: log(%s) is not the absolute path", logpath);
    if(ulogpath && *ulogpath != MYPATHCHR)
      ttservlog(g_serv, TTLOGINFO, "warning: ulog(%s) is not the absolute path", ulogpath);
    if(mport == 0 && mhost && *mhost != MYPATHCHR)
      ttservlog(g_serv, TTLOGINFO, "warning: mhost(%s) is not the absolute path", mhost);
    if(mhost && rtspath && *rtspath != MYPATHCHR)
      ttservlog(g_serv, TTLOGINFO, "warning: rts(%s) is not the absolute path", rtspath);
    if(extpath && *extpath != MYPATHCHR)
      ttservlog(g_serv, TTLOGINFO, "warning: ext(%s) is not the absolute path", extpath);
    if(chdir("/") == -1){
      ttservlog(g_serv, TTLOGERROR, "chdir failed");
      return 1;
    }
  }
  if(dbname && *dbname != '*' && *dbname != '+' && !strstr(dbname, ".tc"))
    ttservlog(g_serv, TTLOGINFO, "warning: dbname(%s) has no suffix for database type", dbname);
  struct stat sbuf;
  if(ulogpath && (stat(ulogpath, &sbuf) != 0 || !S_ISDIR(sbuf.st_mode)))
    ttservlog(g_serv, TTLOGINFO, "warning: ulog(%s) is not a directory", ulogpath);
  if(pidpath){
    char *numstr = tcreadfile(pidpath, -1, NULL);
    if(numstr && kl){
      int64_t pid = tcatoi(numstr);
      tcfree(numstr);
      ttservlog(g_serv, TTLOGINFO,
                "warning: killing the process %lld with SIGTERM", (long long)pid);
      if(kill(pid, SIGTERM) != 0) ttservlog(g_serv, TTLOGERROR, "kill failed");
      int cnt = 0;
      while(true){
        usleep(1000 * 100);
        if((numstr = tcreadfile(pidpath, -1, NULL)) != NULL){
          tcfree(numstr);
        } else {
          break;
        }
        if(++cnt >= 100){
          ttservlog(g_serv, TTLOGINFO,
                    "warning: killing the process %lld with SIGKILL", (long long)pid);
          if(kill(pid, SIGKILL) != 0) ttservlog(g_serv, TTLOGERROR, "kill failed");
          unlink(pidpath);
          break;
        }
      }
      numstr = tcreadfile(pidpath, -1, NULL);
    }
    if(numstr){
      int64_t pid = tcatoi(numstr);
      tcfree(numstr);
      ttservlog(g_serv, TTLOGERROR, "the process %lld may be already running", (long long)pid);
      return 1;
    }
  }
  if(dmn && !ttdaemonize()){
    ttservlog(g_serv, TTLOGERROR, "ttdaemonize failed");
    return 1;
  }
  if(logpath){
    int fd = open(logpath, O_WRONLY | O_APPEND | O_CREAT, 00644);
    if(fd != -1){
      larg.fd = fd;
    } else {
      ttservlog(g_serv, TTLOGERROR, "the log file %s could not be opened", logpath);
      return 1;
    }
  }
  int64_t pid = getpid();
  ttservlog(g_serv, TTLOGSYSTEM, "--------- logging started [%lld] --------", (long long)pid);
  if(pidpath){
    char buf[32];
    sprintf(buf, "%lld\n", (long long)pid);
    if(!tcwritefile(pidpath, buf, strlen(buf))){
      ttservlog(g_serv, TTLOGERROR, "tcwritefile failed");
      return 1;
    }
    ttservlog(g_serv, TTLOGSYSTEM, "process ID configuration: path=%s pid=%lld",
              pidpath, (long long)pid);
  }
  ttservlog(g_serv, TTLOGSYSTEM, "server configuration: host=%s port=%d",
            host ? host : "(any)", port);
  if(!ttservconf(g_serv, host, port)) return 1;
  bool err = false;
  ttservlog(g_serv, TTLOGSYSTEM, "database configuration: name=%s", dbname);
  TCADB *adb = tcadbnew();
  if(!tcadbopen(adb, dbname)){
    err = true;
    ttservlog(g_serv, TTLOGERROR, "tcadbopen failed");
  }
  TCULOG *ulog = tculognew();
  if(ulogpath){
    ttservlog(g_serv, TTLOGSYSTEM, "update log configuration: path=%s limit=%llu async=%d sid=%d",
              ulogpath, (unsigned long long)ulim, uas, sid);
    if(uas && !tculogsetaio(ulog)){
      err = true;
      ttservlog(g_serv, TTLOGERROR, "tculogsetaio failed");
    }
    if(!tculogopen(ulog, ulogpath, ulim)){
      err = true;
      ttservlog(g_serv, TTLOGERROR, "tculogopen failed");
    }
  }
  ttservtune(g_serv, thnum, tout);
  if(mhost)
    ttservlog(g_serv, TTLOGSYSTEM, "replication configuration: host=%s port=%d", mhost, mport);
  void *screxts[thnum];
  TCMDB *scrstash = NULL;
  pthread_mutex_t *scrlcks = NULL;
  if(extpath){
    ttservlog(g_serv, TTLOGSYSTEM, "scripting extension: %s", extpath);
    scrstash = tcmdbnew2(STASHBNUM);
    scrlcks = tcmalloc(sizeof(*scrlcks) * RECMTXNUM);
    for(int i = 0; i < RECMTXNUM; i++){
      pthread_mutexattr_t attr;
      if(pthread_mutexattr_init(&attr) != 0)
        ttservlog(g_serv, TTLOGERROR, "pthread_mutexattr_init failed");
      if(pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK) != 0)
        ttservlog(g_serv, TTLOGERROR, "pthread_mutexattr_settype failed");
      if(pthread_mutex_init(scrlcks + i, &attr) != 0)
        ttservlog(g_serv, TTLOGERROR, "pthread_mutex_init failed");
      pthread_mutexattr_destroy(&attr);
    }
    bool screrr = false;
    for(int i = 0; i < thnum; i++){
      screxts[i] = scrextnew(thnum, i, extpath, adb, ulog, sid, scrstash,
                             scrlcks, RECMTXNUM, do_log, &larg);
      if(!screxts[i]) screrr = true;
    }
    if(screrr){
      err = true;
      ttservlog(g_serv, TTLOGERROR, "scrextnew failed");
    }
  } else {
    for(int i = 0; i < thnum; i++){
      screxts[i] = NULL;
    }
  }
  if(mask != 0)
    ttservlog(g_serv, TTLOGSYSTEM, "command bit mask: 0x%llx", (unsigned long long)mask);
  REPLARG sarg;
  snprintf(sarg.host, TTADDRBUFSIZ, "%s", mhost ? mhost : "");
  sarg.port = mport;
  sarg.rtspath = rtspath;
  sarg.rts = 0;
  sarg.adb = adb;
  sarg.ulog = ulog;
  sarg.sid = sid;
  sarg.fail = false;
  sarg.recon = false;
  if(!(mask & TTMSKSLAVE)) ttservaddtimedhandler(g_serv, 1.0, do_slave, &sarg);
  EXTPCARG *pcargs = NULL;
  int pcnum = 0;
  if(extpath && extpcs){
    pcnum = tclistnum(extpcs) / 2;
    pcargs = tcmalloc(sizeof(*pcargs) * pcnum);
    for(int i = 0; i < pcnum; i++){
      const char *name = tclistval2(extpcs, i * 2);
      double period = tcatof(tclistval2(extpcs, i * 2 + 1));
      EXTPCARG *pcarg = pcargs + i;
      pcarg->name = name;
      pcarg->adb = adb;
      pcarg->ulog = ulog;
      pcarg->sid = sid;
      pcarg->sarg = &sarg;
      pcarg->scrext = scrextnew(thnum, thnum + i, extpath, adb, ulog, sid, scrstash,
                                scrlcks, RECMTXNUM, do_log, &larg);
      if(pcarg->scrext){
        if(*name && period > 0) ttservaddtimedhandler(g_serv, period, do_extpc, pcarg);
      } else {
        err = true;
        ttservlog(g_serv, TTLOGERROR, "scrextnew failed");
      }
    }
  }
  TASKARG targ;
  targ.mask = mask;
  targ.adb = adb;
  targ.ulog = ulog;
  targ.sid = sid;
  targ.sarg = &sarg;
  for(int i = 0; i < RECMTXNUM; i++){
    if(pthread_mutex_init(targ.rmtxs + i, NULL) != 0)
      ttservlog(g_serv, TTLOGERROR, "pthread_mutex_init failed");
  }
  targ.screxts = screxts;
  ttservsettaskhandler(g_serv, do_task, &targ);
  if(larg.fd != 1){
    close(larg.fd);
    larg.fd = 1;
  }
  do {
    g_restart = false;
    if(logpath){
      int fd = open(logpath, O_WRONLY | O_APPEND | O_CREAT, 00644);
      if(fd != -1){
        larg.fd = fd;
      } else {
        err = true;
        ttservlog(g_serv, TTLOGERROR, "open failed");
      }
    }
    if(signal(SIGTERM, sigtermhandler) == SIG_ERR || signal(SIGINT, sigtermhandler) == SIG_ERR ||
       signal(SIGHUP, sigtermhandler) == SIG_ERR || signal(SIGPIPE, SIG_IGN) == SIG_ERR ||
       signal(SIGCHLD, sigchldhandler) == SIG_ERR){
      err = true;
      ttservlog(g_serv, TTLOGERROR, "signal failed");
    }
    if(!ttservstart(g_serv)) err = true;
  } while(g_restart);
  if(pcargs){
    for(int i = 0; i < pcnum; i++){
      EXTPCARG *pcarg = pcargs + i;
      if(pcarg->scrext && !scrextdel(pcarg->scrext)){
        err = true;
        ttservlog(g_serv, TTLOGERROR, "scrextdel failed");
      }
    }
    tcfree(pcargs);
  }
  for(int i = 0; i < RECMTXNUM; i++){
    if(pthread_mutex_destroy(targ.rmtxs + i) != 0)
      ttservlog(g_serv, TTLOGERROR, "pthread_mutex_destroy failed");
  }
  for(int i = 0; i < thnum; i++){
    if(!screxts[i]) continue;
    if(!scrextdel(screxts[i])){
      err = true;
      ttservlog(g_serv, TTLOGERROR, "scrextdel failed");
    }
  }
  if(scrlcks){
    for(int i = 0; i < RECMTXNUM; i++){
      if(pthread_mutex_destroy(scrlcks + i) != 0)
        ttservlog(g_serv, TTLOGERROR, "pthread_mutex_destroy failed");
    }
    tcfree(scrlcks);
  }
  if(scrstash) tcmdbdel(scrstash);
  if(ulogpath && !tculogclose(ulog)){
    err = true;
    ttservlog(g_serv, TTLOGERROR, "tculogclose failed");
  }
  tculogdel(ulog);
  if(!tcadbclose(adb)){
    err = true;
    ttservlog(g_serv, TTLOGERROR, "tcadbclose failed");
  }
  tcadbdel(adb);
  if(pidpath && unlink(pidpath) != 0){
    err = true;
    ttservlog(g_serv, TTLOGERROR, "unlink failed");
  }
  ttservlog(g_serv, TTLOGSYSTEM, "--------- logging finished [%d] --------", pid);
  if(logpath && close(larg.fd) == -1) err = true;
  return err ? 1 : 0;
}


/* handle a log message */
static void do_log(int level, const char *msg, void *opq){
  if(level < g_loglevel) return;
  LOGARG *arg = (LOGARG *)opq;
  char date[48];
  tcdatestrwww(INT64_MAX, INT_MAX, date);
  char buf[LINEBUFSIZ];
  int len = snprintf(buf, LINEBUFSIZ, "%s\t%s\n", date, msg);
  tcwrite(arg ? arg->fd : 1, buf, len);
}


/* replicate master data */
static void do_slave(void *opq){
  REPLARG *arg = opq;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  if(arg->host[0] == '\0' || arg->port < 1) return;
  int rtsfd = open(arg->rtspath, O_RDWR | O_CREAT, 00644);
  if(rtsfd == -1){
    ttservlog(g_serv, TTLOGERROR, "do_slave: open failed");
    return;
  }
  struct stat sbuf;
  if(fstat(rtsfd, &sbuf) == -1){
    ttservlog(g_serv, TTLOGERROR, "do_slave: stat failed");
    close(rtsfd);
    return;
  }
  char rtsbuf[NUMBUFSIZ];
  memset(rtsbuf, 0, NUMBUFSIZ);
  arg->rts = 0;
  if(sbuf.st_size > 0 && tcread(rtsfd, rtsbuf, tclmin(NUMBUFSIZ - 1, sbuf.st_size)))
    arg->rts = strtoll(rtsbuf, NULL, 10);
  TCREPL *repl = tcreplnew();
  pthread_cleanup_push((void (*)(void *))tcrepldel, repl);
  if(tcreplopen(repl, arg->host, arg->port, arg->rts + 1, sid)){
    ttservlog(g_serv, TTLOGINFO, "replicating from %s:%d after %llu",
              arg->host, arg->port, (unsigned long long)arg->rts);
    arg->fail = false;
    arg->recon = false;
    bool err = false;
    uint32_t rsid;
    const char *rbuf;
    int rsiz;
    uint64_t rts;
    while(!err && !ttserviskilled(g_serv) && !arg->recon &&
          (rbuf = tcreplread(repl, &rsiz, &rts, &rsid)) != NULL){
      if(rsiz < 1) continue;
      if(!tculogadbredo(adb, rbuf, rsiz, true, ulog, rsid)){
        err = true;
        ttservlog(g_serv, TTLOGERROR, "do_slave: tculogadbredo failed");
      }
      if(lseek(rtsfd, 0, SEEK_SET) != -1){
        int len = sprintf(rtsbuf, "%llu\n", (unsigned long long)rts);
        if(tcwrite(rtsfd, rtsbuf, len)){
          arg->rts = rts;
        } else {
          err = true;
          ttservlog(g_serv, TTLOGERROR, "do_slave: tcwrite failed");
        }
      } else {
        err = true;
        ttservlog(g_serv, TTLOGERROR, "do_slave: lseek failed");
      }
    }
    tcreplclose(repl);
    ttservlog(g_serv, TTLOGINFO, "replication finished");
  } else {
    if(!arg->fail) ttservlog(g_serv, TTLOGERROR, "do_slave: tcreplopen failed");
    arg->fail = true;
  }
  pthread_cleanup_pop(1);
  if(close(rtsfd) == -1) ttservlog(g_serv, TTLOGERROR, "do_slave: close failed");
}


/* perform an extension command */
static void do_extpc(void *opq){
  EXTPCARG *arg = (EXTPCARG *)opq;
  const char *name = arg->name;
  void *scr = arg->scrext;
  int xsiz;
  char *xbuf = scrextcallmethod(scr, name, "", 0, "", 0, &xsiz);
  tcfree(xbuf);
}


/* handle a task and dispatch it */
static void do_task(TTSOCK *sock, void *opq, TTREQ *req){
  TASKARG *arg = (TASKARG *)opq;
  int c = ttsockgetc(sock);
  if(c == TTMAGICNUM){
    switch(ttsockgetc(sock)){
    case TTCMDPUT:
      do_put(sock, arg, req);
      break;
    case TTCMDPUTKEEP:
      do_putkeep(sock, arg, req);
      break;
    case TTCMDPUTCAT:
      do_putcat(sock, arg, req);
      break;
    case TTCMDPUTSHL:
      do_putshl(sock, arg, req);
      break;
    case TTCMDPUTNR:
      do_putnr(sock, arg, req);
      break;
    case TTCMDOUT:
      do_out(sock, arg, req);
      break;
    case TTCMDGET:
      do_get(sock, arg, req);
      break;
    case TTCMDMGET:
      do_mget(sock, arg, req);
      break;
    case TTCMDVSIZ:
      do_vsiz(sock, arg, req);
      break;
    case TTCMDITERINIT:
      do_iterinit(sock, arg, req);
      break;
    case TTCMDITERNEXT:
      do_iternext(sock, arg, req);
      break;
    case TTCMDFWMKEYS:
      do_fwmkeys(sock, arg, req);
      break;
    case TTCMDADDINT:
      do_addint(sock, arg, req);
      break;
    case TTCMDADDDOUBLE:
      do_adddouble(sock, arg, req);
      break;
    case TTCMDEXT:
      do_ext(sock, arg, req);
      break;
    case TTCMDSYNC:
      do_sync(sock, arg, req);
      break;
    case TTCMDVANISH:
      do_vanish(sock, arg, req);
      break;
    case TTCMDCOPY:
      do_copy(sock, arg, req);
      break;
    case TTCMDRESTORE:
      do_restore(sock, arg, req);
      break;
    case TTCMDSETMST:
      do_setmst(sock, arg, req);
      break;
    case TTCMDRNUM:
      do_rnum(sock, arg, req);
      break;
    case TTCMDSIZE:
      do_size(sock, arg, req);
      break;
    case TTCMDSTAT:
      do_stat(sock, arg, req);
      break;
    case TTCMDMISC:
      do_misc(sock, arg, req);
      break;
    case TTCMDREPL:
      do_repl(sock, arg, req);
      break;
    default:
      ttservlog(g_serv, TTLOGINFO, "unknown command");
      break;
    }
  } else {
    ttsockungetc(sock, c);
    char *line = ttsockgets2(sock);
    if(line){
      int tnum;
      char **tokens = tokenize(line, &tnum);
      if(tnum > 0){
        const char *cmd = tokens[0];
        if(!strcmp(cmd, "set")){
          do_mc_set(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "add")){
          do_mc_add(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "replace")){
          do_mc_replace(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "get") || !strcmp(cmd, "gets")){
          do_mc_get(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "delete")){
          do_mc_delete(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "incr")){
          do_mc_incr(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "decr")){
          do_mc_decr(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "stats")){
          do_mc_stats(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "flush_all")){
          do_mc_flushall(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "version")){
          do_mc_version(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "quit")){
          do_mc_quit(sock, arg, req, tokens, tnum);
        } else if(tnum > 2 && tcstrfwm(tokens[2], "HTTP/1.")){
          int ver = tcatoi(tokens[2] + 7);
          const char *uri = tokens[1];
          if(tcstrifwm(uri, "http://")){
            const char *pv = strchr(uri + 7, '/');
            if(pv) uri = pv;
          }
          if(!strcmp(cmd, "GET")){
            do_http_get(sock, arg, req, ver, uri);
          } else if(!strcmp(cmd, "HEAD")){
            do_http_head(sock, arg, req, ver, uri);
          } else if(!strcmp(cmd, "PUT")){
            do_http_put(sock, arg, req, ver, uri);
          } else if(!strcmp(cmd, "POST")){
            do_http_post(sock, arg, req, ver, uri);
          } else if(!strcmp(cmd, "DELETE")){
            do_http_delete(sock, arg, req, ver, uri);
          }
        }
      }
      tcfree(tokens);
      tcfree(line);
    }
  }
}


/* tokenize a string */
static char **tokenize(char *str, int *np){
  int anum = TOKENUNIT;
  char **tokens = tcmalloc(sizeof(*tokens) * anum);
  int tnum = 0;
  while(*str == ' ' || *str == '\t'){
    str++;
  }
  while(*str != '\0'){
    if(tnum >= anum){
      anum *= 2;
      tokens = tcrealloc(tokens, sizeof(*tokens) * anum);
    }
    tokens[tnum++] = str;
    while(*str != '\0' && *str != ' ' && *str != '\t'){
      str++;
    }
    while(*str == ' ' || *str == '\t'){
      *(str++) = '\0';
    }
  }
  *np = tnum;
  return tokens;
}


/* get the mutex index of a record */
static uint32_t recmtxidx(const char *kbuf, int ksiz){
  uint32_t hash = 725;
  while(ksiz--){
    hash = hash * 29 + *(uint8_t *)kbuf++;
  }
  return hash % RECMTXNUM;
}


/* handle the put command */
static void do_put(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing put command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  int ksiz = ttsockgetint32(sock);
  int vsiz = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || ksiz < 0 || ksiz > MAXARGSIZ || vsiz < 0 || vsiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_put: invalid parameters");
    return;
  }
  int rsiz = ksiz + vsiz;
  char stack[TTIOBUFSIZ];
  char *buf = (rsiz < TTIOBUFSIZ) ? stack : tcmalloc(rsiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, rsiz) && !ttsockcheckend(sock)){
    uint8_t code = 0;
    if(mask & (TTMSKPUT | TTMSKALLORG | TTMSKALLWRITE)){
      code = 1;
      ttservlog(g_serv, TTLOGINFO, "do_put: forbidden");
    } else if(!tculogadbput(ulog, sid, adb, buf, ksiz, buf + ksiz, vsiz)){
      code = 1;
      ttservlog(g_serv, TTLOGERROR, "do_put: operation failed");
    }
    if(ttsocksend(sock, &code, sizeof(code))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_put: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_put: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the putkeep command */
static void do_putkeep(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing putkeep command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  int ksiz = ttsockgetint32(sock);
  int vsiz = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || ksiz < 0 || ksiz > MAXARGSIZ || vsiz < 0 || vsiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_putkeep: invalid parameters");
    return;
  }
  int rsiz = ksiz + vsiz;
  char stack[TTIOBUFSIZ];
  char *buf = (rsiz < TTIOBUFSIZ) ? stack : tcmalloc(rsiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, rsiz) && !ttsockcheckend(sock)){
    uint8_t code = 0;
    if(mask & (TTMSKPUTKEEP | TTMSKALLORG | TTMSKALLWRITE)){
      code = 1;
      ttservlog(g_serv, TTLOGINFO, "do_putkeep: forbidden");
    } else if(!tculogadbputkeep(ulog, sid, adb, buf, ksiz, buf + ksiz, vsiz)){
      code = 1;
    }
    if(ttsocksend(sock, &code, sizeof(code))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_putkeep: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_putkeep: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the putcat command */
static void do_putcat(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing putcat command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  int ksiz = ttsockgetint32(sock);
  int vsiz = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || ksiz < 0 || ksiz > MAXARGSIZ || vsiz < 0 || vsiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_putcat: invalid parameters");
    return;
  }
  int rsiz = ksiz + vsiz;
  char stack[TTIOBUFSIZ];
  char *buf = (rsiz < TTIOBUFSIZ) ? stack : tcmalloc(rsiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, rsiz) && !ttsockcheckend(sock)){
    uint8_t code = 0;
    if(mask & (TTMSKPUTCAT | TTMSKALLORG | TTMSKALLWRITE)){
      code = 1;
      ttservlog(g_serv, TTLOGINFO, "do_putcat: forbidden");
    } else if(!tculogadbputcat(ulog, sid, adb, buf, ksiz, buf + ksiz, vsiz)){
      code = 1;
      ttservlog(g_serv, TTLOGERROR, "do_putcat: operation failed");
    }
    if(ttsocksend(sock, &code, sizeof(code))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_putcat: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_putcat: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the putshl command */
static void do_putshl(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing putshl command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  pthread_mutex_t *rmtxs = arg->rmtxs;
  int ksiz = ttsockgetint32(sock);
  int vsiz = ttsockgetint32(sock);
  int width = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || ksiz < 0 || ksiz > MAXARGSIZ || vsiz < 0 || vsiz > MAXARGSIZ ||
     width < 0){
    ttservlog(g_serv, TTLOGINFO, "do_putshl: invalid parameters");
    return;
  }
  int rsiz = ksiz + vsiz;
  char stack[TTIOBUFSIZ];
  char *buf = (rsiz < TTIOBUFSIZ) ? stack : tcmalloc(rsiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, rsiz) && !ttsockcheckend(sock)){
    uint8_t code = 0;
    int mtxidx = recmtxidx(buf, ksiz);
    if(mask & (TTMSKPUTSHL | TTMSKALLORG | TTMSKALLWRITE)){
      code = 1;
      ttservlog(g_serv, TTLOGINFO, "do_putshl: forbidden");
    } else if(pthread_mutex_lock(rmtxs + mtxidx) == 0){
      int osiz;
      char *obuf = tcadbget(adb, buf, ksiz, &osiz);
      if(obuf){
        obuf = tcrealloc(obuf, osiz + vsiz);
        memcpy(obuf + osiz, buf + ksiz, vsiz);
        osiz += vsiz;
      } else {
        obuf = tcmalloc(vsiz + 1);
        memcpy(obuf, buf + ksiz, vsiz);
        osiz = vsiz;
      }
      const char *nbuf;
      int nsiz;
      if(osiz <= width){
        nbuf = obuf;
        nsiz = osiz;
      } else {
        nbuf = obuf + osiz - width;
        nsiz = width;
      }
      if(!tculogadbput(ulog, sid, adb, buf, ksiz, nbuf, nsiz)){
        code = 1;
        ttservlog(g_serv, TTLOGERROR, "do_putshl: operation failed");
      }
      tcfree(obuf);
      if(pthread_mutex_unlock(rmtxs + mtxidx) != 0)
        ttservlog(g_serv, TTLOGERROR, "do_putshl: pthread_mutex_unlock failed");
    } else {
      code = 1;
      ttservlog(g_serv, TTLOGERROR, "do_putshl: pthread_mutex_lock failed");
    }
    if(ttsocksend(sock, &code, sizeof(code))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_putshl: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_putshl: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the putnr command */
static void do_putnr(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing putnr command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  int ksiz = ttsockgetint32(sock);
  int vsiz = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || ksiz < 0 || ksiz > MAXARGSIZ || vsiz < 0 || vsiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_putnr: invalid parameters");
    return;
  }
  int rsiz = ksiz + vsiz;
  char stack[TTIOBUFSIZ];
  char *buf = (rsiz < TTIOBUFSIZ) ? stack : tcmalloc(rsiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, rsiz) && !ttsockcheckend(sock)){
    uint8_t code = 0;
    if(mask & (TTMSKPUTNR | TTMSKALLORG | TTMSKALLWRITE)){
      code = 1;
      ttservlog(g_serv, TTLOGINFO, "do_putnr: forbidden");
    } else if(!tculogadbput(ulog, sid, adb, buf, ksiz, buf + ksiz, vsiz)){
      code = 1;
      ttservlog(g_serv, TTLOGERROR, "do_putnr: operation failed");
    }
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_putnr: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the out command */
static void do_out(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing out command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  int ksiz = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || ksiz < 0 || ksiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_out: invalid parameters");
    return;
  }
  char stack[TTIOBUFSIZ];
  char *buf = (ksiz < TTIOBUFSIZ) ? stack : tcmalloc(ksiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, ksiz) && !ttsockcheckend(sock)){
    uint8_t code = 0;
    if(mask & (TTMSKOUT | TTMSKALLORG | TTMSKALLWRITE)){
      code = 1;
      ttservlog(g_serv, TTLOGINFO, "do_out: forbidden");
    } else if(!tculogadbout(ulog, sid, adb, buf, ksiz)){
      code = 1;
    }
    if(ttsocksend(sock, &code, sizeof(code))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_out: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_out: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the get command */
static void do_get(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing get command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  int ksiz = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || ksiz < 0 || ksiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_get: invalid parameters");
    return;
  }
  char stack[TTIOBUFSIZ];
  char *buf = (ksiz < TTIOBUFSIZ) ? stack : tcmalloc(ksiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, ksiz) && !ttsockcheckend(sock)){
    char *vbuf;
    int vsiz;
    if(mask & (TTMSKGET | TTMSKALLORG | TTMSKALLREAD)){
      vbuf = NULL;
      vsiz = 0;
      ttservlog(g_serv, TTLOGINFO, "do_get: forbidden");
    } else {
      vbuf = tcadbget(adb, buf, ksiz, &vsiz);
    }
    if(vbuf){
      int rsiz = vsiz + sizeof(uint8_t) + sizeof(uint32_t);
      char *rbuf = (rsiz < TTIOBUFSIZ) ? stack : tcmalloc(rsiz);
      pthread_cleanup_push(free, (rbuf == stack) ? NULL : rbuf);
      *rbuf = 0;
      uint32_t num;
      num = TTHTONL((uint32_t)vsiz);
      memcpy(rbuf + sizeof(uint8_t), &num, sizeof(uint32_t));
      memcpy(rbuf + sizeof(uint8_t) + sizeof(uint32_t), vbuf, vsiz);
      tcfree(vbuf);
      if(ttsocksend(sock, rbuf, rsiz)){
        req->keep = true;
      } else {
        ttservlog(g_serv, TTLOGINFO, "do_get: response failed");
      }
      pthread_cleanup_pop(1);
    } else {
      uint8_t code = 1;
      if(ttsocksend(sock, &code, sizeof(code))){
        req->keep = true;
      } else {
        ttservlog(g_serv, TTLOGINFO, "do_get: response failed");
      }
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_get: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the mget command */
static void do_mget(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing mget command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  int rnum = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || rnum < 0 || rnum > MAXARGNUM){
    ttservlog(g_serv, TTLOGINFO, "do_mget: invalid parameters");
    return;
  }
  TCLIST *keys = tclistnew2(rnum);
  pthread_cleanup_push((void (*)(void *))tclistdel, keys);
  char stack[TTIOBUFSIZ];
  for(int i = 0; i < rnum; i++){
    int ksiz = ttsockgetint32(sock);
    if(ttsockcheckend(sock) || ksiz < 0 || ksiz > MAXARGSIZ) break;
    char *buf = (ksiz < TTIOBUFSIZ) ? stack : tcmalloc(ksiz + 1);
    pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
    if(ttsockrecv(sock, buf, ksiz)) tclistpush(keys, buf, ksiz);
    pthread_cleanup_pop(1);
  }
  if(!ttsockcheckend(sock)){
    TCXSTR *xstr = tcxstrnew();
    pthread_cleanup_push((void (*)(void *))tcxstrdel, xstr);
    uint8_t code = 0;
    tcxstrcat(xstr, &code, sizeof(code));
    uint32_t num = 0;
    tcxstrcat(xstr, &num, sizeof(num));
    rnum = 0;
    if(mask & (TTMSKMGET | TTMSKALLORG | TTMSKALLREAD)){
      ttservlog(g_serv, TTLOGINFO, "do_mget: forbidden");
    } else {
      for(int i = 0; i < tclistnum(keys); i++){
        int ksiz;
        const char *kbuf = tclistval(keys, i, &ksiz);
        int vsiz;
        char *vbuf = tcadbget(adb, kbuf, ksiz, &vsiz);
        if(vbuf){
          num = TTHTONL((uint32_t)ksiz);
          tcxstrcat(xstr, &num, sizeof(num));
          num = TTHTONL((uint32_t)vsiz);
          tcxstrcat(xstr, &num, sizeof(num));
          tcxstrcat(xstr, kbuf, ksiz);
          tcxstrcat(xstr, vbuf, vsiz);
          tcfree(vbuf);
          rnum++;
        }
      }
    }
    num = TTHTONL((uint32_t)rnum);
    *(uint32_t *)((char *)tcxstrptr(xstr) + sizeof(code)) = num;
    if(ttsocksend(sock, tcxstrptr(xstr), tcxstrsize(xstr))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_mget: response failed");
    }
    pthread_cleanup_pop(1);
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mget: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the vsiz command */
static void do_vsiz(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing vsiz command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  int ksiz = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || ksiz < 0 || ksiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_vsiz: invalid parameters");
    return;
  }
  char stack[TTIOBUFSIZ];
  char *buf = (ksiz < TTIOBUFSIZ) ? stack : tcmalloc(ksiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, ksiz) && !ttsockcheckend(sock)){
    int vsiz;
    if(mask & (TTMSKVSIZ | TTMSKALLORG | TTMSKALLREAD)){
      vsiz = -1;
      ttservlog(g_serv, TTLOGINFO, "do_vsiz: forbidden");
    } else {
      vsiz = tcadbvsiz(adb, buf, ksiz);
    }
    if(vsiz >= 0){
      *stack = 0;
      uint32_t num;
      num = TTHTONL((uint32_t)vsiz);
      memcpy(stack + sizeof(uint8_t), &num, sizeof(uint32_t));
      if(ttsocksend(sock, stack, sizeof(uint8_t) + sizeof(uint32_t))){
        req->keep = true;
      } else {
        ttservlog(g_serv, TTLOGINFO, "do_vsiz: response failed");
      }
    } else {
      uint8_t code = 1;
      if(ttsocksend(sock, &code, sizeof(code))){
        req->keep = true;
      } else {
        ttservlog(g_serv, TTLOGINFO, "do_vsiz: response failed");
      }
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_vsiz: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the iterinit command */
static void do_iterinit(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing iterinit command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  uint8_t code = 0;
  if(mask & (TTMSKITERINIT | TTMSKALLORG | TTMSKALLREAD)){
    code = 1;
    ttservlog(g_serv, TTLOGINFO, "do_iterinit: forbidden");
  } else if(!tcadbiterinit(adb)){
    code = 1;
    ttservlog(g_serv, TTLOGERROR, "do_iterinit: operation failed");
  }
  if(ttsocksend(sock, &code, sizeof(code))){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_iterinit: response failed");
  }
}


/* handle the iternext command */
static void do_iternext(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing iternext command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  int vsiz;
  char *vbuf;
  if(mask & (TTMSKITERNEXT | TTMSKALLORG | TTMSKALLREAD)){
    vbuf = NULL;
    vsiz = 0;
    ttservlog(g_serv, TTLOGINFO, "do_iternext: forbidden");
  } else {
    vbuf = tcadbiternext(adb, &vsiz);
  }
  if(vbuf){
    int rsiz = vsiz + sizeof(uint8_t) + sizeof(uint32_t);
    char stack[TTIOBUFSIZ];
    char *rbuf = (rsiz < TTIOBUFSIZ) ? stack : tcmalloc(rsiz);
    pthread_cleanup_push(free, (rbuf == stack) ? NULL : rbuf);
    *rbuf = 0;
    uint32_t num;
    num = TTHTONL((uint32_t)vsiz);
    memcpy(rbuf + sizeof(uint8_t), &num, sizeof(uint32_t));
    memcpy(rbuf + sizeof(uint8_t) + sizeof(uint32_t), vbuf, vsiz);
    tcfree(vbuf);
    if(ttsocksend(sock, rbuf, rsiz)){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_iternext: response failed");
    }
    pthread_cleanup_pop(1);
  } else {
    uint8_t code = 1;
    if(ttsocksend(sock, &code, sizeof(code))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_iternext: response failed");
    }
  }
}


/* handle the fwmkeys command */
static void do_fwmkeys(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing fwmkeys command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  int psiz = ttsockgetint32(sock);
  int max = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || psiz < 0 || psiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_fwmkeys: invalid parameters");
    return;
  }
  char stack[TTIOBUFSIZ];
  char *buf = (psiz < TTIOBUFSIZ) ? stack : tcmalloc(psiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, psiz) && !ttsockcheckend(sock)){
    TCLIST *keys = tcadbfwmkeys(adb, buf, psiz, max);
    pthread_cleanup_push((void (*)(void *))tclistdel, keys);
    TCXSTR *xstr = tcxstrnew();
    pthread_cleanup_push((void (*)(void *))tcxstrdel, xstr);
    uint8_t code = 0;
    tcxstrcat(xstr, &code, sizeof(code));
    uint32_t num = 0;
    tcxstrcat(xstr, &num, sizeof(num));
    int knum = 0;
    if(mask & (TTMSKFWMKEYS | TTMSKALLORG | TTMSKALLREAD)){
      ttservlog(g_serv, TTLOGINFO, "do_fwmkeys: forbidden");
    } else {
      for(int i = 0; i < tclistnum(keys); i++){
        int ksiz;
        const char *kbuf = tclistval(keys, i, &ksiz);
        num = TTHTONL((uint32_t)ksiz);
        tcxstrcat(xstr, &num, sizeof(num));
        tcxstrcat(xstr, kbuf, ksiz);
        knum++;
      }
    }
    num = TTHTONL((uint32_t)knum);
    *(uint32_t *)((char *)tcxstrptr(xstr) + sizeof(code)) = num;
    if(ttsocksend(sock, tcxstrptr(xstr), tcxstrsize(xstr))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_fwmkeys: response failed");
    }
    pthread_cleanup_pop(1);
    pthread_cleanup_pop(1);
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_fwmkeys: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the addint command */
static void do_addint(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing addint command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  int ksiz = ttsockgetint32(sock);
  int anum = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || ksiz < 0 || ksiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_addint: invalid parameters");
    return;
  }
  char stack[TTIOBUFSIZ];
  char *buf = (ksiz < TTIOBUFSIZ) ? stack : tcmalloc(ksiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, ksiz) && !ttsockcheckend(sock)){
    int snum;
    if(mask & (TTMSKADDINT | TTMSKALLORG | TTMSKALLWRITE)){
      snum = INT_MIN;
      ttservlog(g_serv, TTLOGINFO, "do_addint: forbidden");
    } else {
      snum = tculogadbaddint(ulog, sid, adb, buf, ksiz, anum);
    }
    if(snum != INT_MIN){
      *stack = 0;
      uint32_t num;
      num = TTHTONL((uint32_t)snum);
      memcpy(stack + sizeof(uint8_t), &num, sizeof(uint32_t));
      if(ttsocksend(sock, stack, sizeof(uint8_t) + sizeof(uint32_t))){
        req->keep = true;
      } else {
        ttservlog(g_serv, TTLOGINFO, "do_addint: response failed");
      }
    } else {
      uint8_t code = 1;
      if(ttsocksend(sock, &code, sizeof(code))){
        req->keep = true;
      } else {
        ttservlog(g_serv, TTLOGINFO, "do_addint: response failed");
      }
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_addint: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the adddouble command */
static void do_adddouble(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing adddouble command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  int ksiz = ttsockgetint32(sock);
  char abuf[sizeof(uint64_t)*2];
  if(!ttsockrecv(sock, abuf, sizeof(abuf)) || ttsockcheckend(sock) ||
     ksiz < 0 || ksiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_adddouble: invalid parameters");
    return;
  }
  double anum = ttunpackdouble(abuf);
  char stack[TTIOBUFSIZ];
  char *buf = (ksiz < TTIOBUFSIZ) ? stack : tcmalloc(ksiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, ksiz) && !ttsockcheckend(sock)){
    double snum;
    if(mask & (TTMSKADDDOUBLE | TTMSKALLORG | TTMSKALLWRITE)){
      snum = nan("");
      ttservlog(g_serv, TTLOGINFO, "do_adddouble: forbidden");
    } else {
      snum = tculogadbadddouble(ulog, sid, adb, buf, ksiz, anum);
    }
    if(!isnan(snum)){
      *stack = 0;
      ttpackdouble(snum, abuf);
      memcpy(stack + sizeof(uint8_t), abuf, sizeof(abuf));
      if(ttsocksend(sock, stack, sizeof(uint8_t) + sizeof(abuf))){
        req->keep = true;
      } else {
        ttservlog(g_serv, TTLOGINFO, "do_adddouble: response failed");
      }
    } else {
      uint8_t code = 1;
      if(ttsocksend(sock, &code, sizeof(code))){
        req->keep = true;
      } else {
        ttservlog(g_serv, TTLOGINFO, "do_adddouble: response failed");
      }
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_adddouble: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the ext command */
static void do_ext(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing ext command");
  uint64_t mask = arg->mask;
  pthread_mutex_t *rmtxs = arg->rmtxs;
  void *scr = arg->screxts[req->idx];
  int nsiz = ttsockgetint32(sock);
  int opts = ttsockgetint32(sock);
  int ksiz = ttsockgetint32(sock);
  int vsiz = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || nsiz < 0 || nsiz >= TTADDRBUFSIZ ||
     ksiz < 0 || ksiz > MAXARGSIZ || vsiz < 0 || vsiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_ext: invalid parameters");
    return;
  }
  int rsiz = nsiz + ksiz + vsiz;
  char stack[TTIOBUFSIZ];
  char *buf = (rsiz < TTIOBUFSIZ) ? stack : tcmalloc(rsiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, rsiz) && !ttsockcheckend(sock)){
    char name[TTADDRBUFSIZ];
    memcpy(name, buf, nsiz);
    name[nsiz] = '\0';
    const char *kbuf = buf + nsiz;
    const char *vbuf = kbuf + ksiz;
    int xsiz = 0;
    char *xbuf = NULL;
    if(mask & (TTMSKEXT | TTMSKALLORG)){
      ttservlog(g_serv, TTLOGINFO, "do_ext: forbidden");
    } else if(scr){
      if(opts & RDBXOLCKGLB){
        bool err = false;
        for(int i = 0; i < RECMTXNUM; i++){
          if(pthread_mutex_lock(rmtxs + i) != 0){
            ttservlog(g_serv, TTLOGERROR, "do_ext: pthread_mutex_lock failed");
            while(--i >= 0){
              pthread_mutex_unlock(rmtxs + i);
            }
            err = true;
            break;
          }
        }
        if(!err){
          xbuf = scrextcallmethod(scr, name, kbuf, ksiz, vbuf, vsiz, &xsiz);
          for(int i = RECMTXNUM - 1; i >= 0; i--){
            if(pthread_mutex_unlock(rmtxs + i) != 0)
              ttservlog(g_serv, TTLOGERROR, "do_ext: pthread_mutex_unlock failed");
          }
        }
      } else if(opts & RDBXOLCKREC){
        int mtxidx = recmtxidx(kbuf, ksiz);
        if(pthread_mutex_lock(rmtxs + mtxidx) == 0){
          xbuf = scrextcallmethod(scr, name, kbuf, ksiz, vbuf, vsiz, &xsiz);
          if(pthread_mutex_unlock(rmtxs + mtxidx) != 0)
            ttservlog(g_serv, TTLOGERROR, "do_ext: pthread_mutex_unlock failed");
        } else {
          ttservlog(g_serv, TTLOGERROR, "do_ext: pthread_mutex_lock failed");
        }
      } else {
        xbuf = scrextcallmethod(scr, name, kbuf, ksiz, vbuf, vsiz, &xsiz);
      }
    }
    if(xbuf){
      int rsiz = xsiz + sizeof(uint8_t) + sizeof(uint32_t);
      char *rbuf = (rsiz < TTIOBUFSIZ) ? stack : tcmalloc(rsiz);
      pthread_cleanup_push(free, (rbuf == stack) ? NULL : rbuf);
      *rbuf = 0;
      uint32_t num;
      num = TTHTONL((uint32_t)xsiz);
      memcpy(rbuf + sizeof(uint8_t), &num, sizeof(uint32_t));
      memcpy(rbuf + sizeof(uint8_t) + sizeof(uint32_t), xbuf, xsiz);
      tcfree(xbuf);
      if(ttsocksend(sock, rbuf, rsiz)){
        req->keep = true;
      } else {
        ttservlog(g_serv, TTLOGINFO, "do_ext: response failed");
      }
      pthread_cleanup_pop(1);
    } else {
      uint8_t code = 1;
      if(ttsocksend(sock, &code, sizeof(code))){
        req->keep = true;
      } else {
        ttservlog(g_serv, TTLOGINFO, "do_ext: response failed");
      }
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_ext: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the sync command */
static void do_sync(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGINFO, "doing sync command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  uint8_t code = 0;
  if(mask & (TTMSKSYNC | TTMSKALLORG | TTMSKALLMANAGE)){
    code = 1;
    ttservlog(g_serv, TTLOGINFO, "do_sync: forbidden");
  } else if(!tcadbsync(adb)){
    code = 1;
    ttservlog(g_serv, TTLOGERROR, "do_sync: operation failed");
  }
  if(ttsocksend(sock, &code, sizeof(code))){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_sync: response failed");
  }
}


/* handle the vanish command */
static void do_vanish(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGINFO, "doing vanish command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  uint8_t code = 0;
  if(mask & (TTMSKVANISH | TTMSKALLORG | TTMSKALLWRITE)){
    code = 1;
    ttservlog(g_serv, TTLOGINFO, "do_vanish: forbidden");
  } else if(!tculogadbvanish(ulog, sid, adb)){
    code = 1;
    ttservlog(g_serv, TTLOGERROR, "do_vanish: operation failed");
  }
  if(ttsocksend(sock, &code, sizeof(code))){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_vanish: response failed");
  }
}


/* handle the copy command */
static void do_copy(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGINFO, "doing copy command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  int psiz = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || psiz < 0 || psiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_copy: invalid parameters");
    return;
  }
  char stack[TTIOBUFSIZ];
  char *buf = (psiz < TTIOBUFSIZ) ? stack : tcmalloc(psiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, psiz) && !ttsockcheckend(sock)){
    buf[psiz] = '\0';
    uint8_t code = 0;
    if(mask & (TTMSKCOPY | TTMSKALLORG | TTMSKALLMANAGE)){
      code = 1;
      ttservlog(g_serv, TTLOGINFO, "do_copy: forbidden");
    } else if(!tcadbcopy(adb, buf)){
      code = 1;
      ttservlog(g_serv, TTLOGERROR, "do_copy: operation failed");
    }
    if(ttsocksend(sock, &code, sizeof(code))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_copy: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_copy: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the restore command */
static void do_restore(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGINFO, "doing restore command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  int psiz = ttsockgetint32(sock);
  uint64_t ts = ttsockgetint64(sock);
  if(ttsockcheckend(sock) || psiz < 0 || psiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_restore: invalid parameters");
    return;
  }
  char stack[TTIOBUFSIZ];
  char *buf = (psiz < TTIOBUFSIZ) ? stack : tcmalloc(psiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, psiz) && !ttsockcheckend(sock)){
    buf[psiz] = '\0';
    bool con = true;
    if(*buf == '+'){
      con = false;
      buf++;
    }
    uint8_t code = 0;
    if(mask & (TTMSKRESTORE | TTMSKALLORG | TTMSKALLMANAGE)){
      code = 1;
      ttservlog(g_serv, TTLOGINFO, "do_restore: forbidden");
    } else if(!tculogadbrestore(adb, buf, ts, con, ulog)){
      code = 1;
      ttservlog(g_serv, TTLOGERROR, "do_restore: operation failed");
    }
    if(ttsocksend(sock, &code, sizeof(code))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_restore: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_restore: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the setmst command */
static void do_setmst(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGINFO, "doing setmst command");
  uint64_t mask = arg->mask;
  REPLARG *sarg = arg->sarg;
  int hsiz = ttsockgetint32(sock);
  int port = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || hsiz < 0 || hsiz > MAXARGSIZ || port < 0){
    ttservlog(g_serv, TTLOGINFO, "do_setmst: invalid parameters");
    return;
  }
  char stack[TTIOBUFSIZ];
  char *buf = (hsiz < TTIOBUFSIZ) ? stack : tcmalloc(hsiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, hsiz) && !ttsockcheckend(sock)){
    buf[hsiz] = '\0';
    uint8_t code = 0;
    if(mask & (TTMSKSETMST | TTMSKALLORG | TTMSKALLMANAGE)){
      code = 1;
      ttservlog(g_serv, TTLOGINFO, "do_setmst: forbidden");
    } else {
      snprintf(sarg->host, TTADDRBUFSIZ, "%s", buf);
      sarg->port = port;
      sarg->recon = true;
    }
    if(ttsocksend(sock, &code, sizeof(code))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_setmst: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_setmst: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the rnum command */
static void do_rnum(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing rnum command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  char buf[LINEBUFSIZ];
  *buf = 0;
  uint64_t rnum;
  if(mask & (TTMSKRNUM | TTMSKALLORG | TTMSKALLREAD)){
    rnum = 0;
    ttservlog(g_serv, TTLOGINFO, "do_rnum: forbidden");
  } else {
    rnum = tcadbrnum(adb);
  }
  rnum = TTHTONLL(rnum);
  memcpy(buf + sizeof(uint8_t), &rnum, sizeof(uint64_t));
  if(ttsocksend(sock, buf, sizeof(uint8_t) + sizeof(uint64_t))){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_rnum: response failed");
  }
}


/* handle the size command */
static void do_size(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing size command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  char buf[LINEBUFSIZ];
  *buf = 0;
  uint64_t size;
  if(mask & (TTMSKSIZE | TTMSKALLORG | TTMSKALLREAD)){
    size = 0;
    ttservlog(g_serv, TTLOGINFO, "do_size: forbidden");
  } else {
    size = tcadbsize(adb);
  }
  size = TTHTONLL(size);
  memcpy(buf + sizeof(uint8_t), &size, sizeof(uint64_t));
  if(ttsocksend(sock, buf, sizeof(uint8_t) + sizeof(uint64_t))){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_size: response failed");
  }
}


/* handle the stat command */
static void do_stat(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing stat command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  REPLARG *sarg = arg->sarg;
  char buf[TTIOBUFSIZ];
  char *wp = buf + sizeof(uint8_t) + sizeof(uint32_t);
  if(mask & (TTMSKSTAT | TTMSKALLORG | TTMSKALLREAD)){
    ttservlog(g_serv, TTLOGINFO, "do_stat: forbidden");
  } else {
    double now = tctime();
    wp += sprintf(wp, "version\t%s\n", ttversion);
    wp += sprintf(wp, "time\t%.6f\n", now);
    wp += sprintf(wp, "pid\t%lld\n", (long long)getpid());
    wp += sprintf(wp, "sid\t%d\n", arg->sid);
    switch(tcadbomode(adb)){
    case ADBOVOID: wp += sprintf(wp, "type\tvoid\n"); break;
    case ADBOMDB: wp += sprintf(wp, "type\ton-memory hash\n"); break;
    case ADBONDB: wp += sprintf(wp, "type\ton-memory tree\n"); break;
    case ADBOHDB: wp += sprintf(wp, "type\thash\n"); break;
    case ADBOBDB: wp += sprintf(wp, "type\tB+ tree\n"); break;
    case ADBOFDB: wp += sprintf(wp, "type\tfixed-length\n"); break;
    case ADBOTDB: wp += sprintf(wp, "type\ttable\n"); break;
    }
    wp += sprintf(wp, "rnum\t%llu\n", (unsigned long long)tcadbrnum(adb));
    wp += sprintf(wp, "size\t%llu\n", (unsigned long long)tcadbsize(adb));
    wp += sprintf(wp, "bigend\t%d\n", TTBIGEND);
    if(sarg->host[0] != '\0'){
      wp += sprintf(wp, "mhost\t%s\n", sarg->host);
      wp += sprintf(wp, "mport\t%d\n", sarg->port);
      wp += sprintf(wp, "rts\t%llu\n", (unsigned long long)sarg->rts);
      double delay = now - sarg->rts / 1000000.0;
      wp += sprintf(wp, "delay\t%.6f\n", delay >= 0 ? delay : 0.0);
    }
    wp += sprintf(wp, "fd\t%d\n", sock->fd);
    wp += sprintf(wp, "loadavg\t%.6f\n", ttgetloadavg());
    wp += sprintf(wp, "ru_real\t%.6f\n", now - g_starttime);
    struct rusage ubuf;
    memset(&ubuf, 0, sizeof(ubuf));
    if(getrusage(RUSAGE_SELF, &ubuf) == 0){
      wp += sprintf(wp, "ru_user\t%d.%06d\n",
                    (int)ubuf.ru_utime.tv_sec, (int)ubuf.ru_utime.tv_usec);
      wp += sprintf(wp, "ru_sys\t%d.%06d\n",
                    (int)ubuf.ru_stime.tv_sec, (int)ubuf.ru_stime.tv_usec);
    }
  }
  *buf = 0;
  uint32_t size = wp - buf - (sizeof(uint8_t) + sizeof(uint32_t));
  size = TTHTONL(size);
  memcpy(buf + sizeof(uint8_t), &size, sizeof(uint32_t));
  if(ttsocksend(sock, buf, wp - buf)){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_stat: response failed");
  }
}


/* handle the misc command */
static void do_misc(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing misc command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  int nsiz = ttsockgetint32(sock);
  int opts = ttsockgetint32(sock);
  int rnum = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || nsiz < 0 || nsiz >= TTADDRBUFSIZ || rnum < 0 || rnum > MAXARGNUM){
    ttservlog(g_serv, TTLOGINFO, "do_misc: invalid parameters");
    return;
  }
  char name[TTADDRBUFSIZ];
  if(!ttsockrecv(sock, name, nsiz) && !ttsockcheckend(sock)){
    ttservlog(g_serv, TTLOGINFO, "do_misc: invalid parameters");
    return;
  }
  name[nsiz] = '\0';
  TCLIST *args = tclistnew2(rnum);
  pthread_cleanup_push((void (*)(void *))tclistdel, args);
  char stack[TTIOBUFSIZ];
  for(int i = 0; i < rnum; i++){
    int rsiz = ttsockgetint32(sock);
    if(ttsockcheckend(sock) || rsiz < 0 || rsiz > MAXARGSIZ) break;
    char *buf = (rsiz < TTIOBUFSIZ) ? stack : tcmalloc(rsiz + 1);
    pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
    if(ttsockrecv(sock, buf, rsiz)) tclistpush(args, buf, rsiz);
    pthread_cleanup_pop(1);
  }
  if(!ttsockcheckend(sock)){
    TCXSTR *xstr = tcxstrnew();
    pthread_cleanup_push((void (*)(void *))tcxstrdel, xstr);
    uint8_t code = 0;
    tcxstrcat(xstr, &code, sizeof(code));
    uint32_t num = 0;
    tcxstrcat(xstr, &num, sizeof(num));
    rnum = 0;
    if(mask & (TTMSKMISC | TTMSKALLORG | TTMSKALLWRITE)){
      ttservlog(g_serv, TTLOGINFO, "do_misc: forbidden");
    } else {
      TCLIST *res = (opts & RDBMONOULOG) ?
        tcadbmisc(adb, name, args) : tculogadbmisc(ulog, sid, adb, name, args);
      if(res){
        for(int i = 0; i < tclistnum(res); i++){
          int esiz;
          const char *ebuf = tclistval(res, i, &esiz);
          num = TTHTONL((uint32_t)esiz);
          tcxstrcat(xstr, &num, sizeof(num));
          tcxstrcat(xstr, ebuf, esiz);
          rnum++;
        }
        tclistdel(res);
      } else {
        *(uint8_t *)tcxstrptr(xstr) = 1;
      }
    }
    num = TTHTONL((uint32_t)rnum);
    *(uint32_t *)((char *)tcxstrptr(xstr) + sizeof(code)) = num;
    if(ttsocksend(sock, tcxstrptr(xstr), tcxstrsize(xstr))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_misc: response failed");
    }
    pthread_cleanup_pop(1);
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_misc: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the repl command */
static void do_repl(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGINFO, "doing repl command");
  uint64_t mask = arg->mask;
  TCULOG *ulog = arg->ulog;
  uint64_t ts = ttsockgetint64(sock);
  uint64_t sid = ttsockgetint32(sock);
  if(ttsockcheckend(sock)){
    ttservlog(g_serv, TTLOGINFO, "do_repl: invalid parameters");
    return;
  }
  if(mask & TTMSKREPL){
    ttservlog(g_serv, TTLOGINFO, "do_repl: forbidden");
    return;
  }
  TCULRD *ulrd = tculrdnew(ulog, ts);
  if(ulrd){
    pthread_cleanup_push((void (*)(void *))tculrddel, ulrd);
    bool err = false;
    bool idle = true;
    const char *rbuf;
    int rsiz;
    uint64_t rts;
    uint32_t rsid;
    char stack[TTIOBUFSIZ];
    while(!err && !ttserviskilled(g_serv)){
      ttsocksetlife(sock, UINT_MAX);
      req->mtime = tctime() + UINT_MAX;
      if(idle){
        *(unsigned char *)stack = TCULMAGICNOP;
        if(!ttsocksend(sock, stack, sizeof(uint8_t))){
          err = true;
          ttservlog(g_serv, TTLOGINFO, "do_repl: connection closed");
        }
      }
      tculrdwait(ulrd);
      idle = true;
      while(!err && (rbuf = tculrdread(ulrd, &rsiz, &rts, &rsid)) != NULL){
        idle = false;
        if(rsid == sid) continue;
        int msiz = sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint32_t) * 2 + rsiz;
        char *mbuf = (msiz < TTIOBUFSIZ) ? stack : tcmalloc(msiz);
        pthread_cleanup_push(free, (mbuf == stack) ? NULL : mbuf);
        unsigned char *wp = (unsigned char *)mbuf;
        *(wp++) = TCULMAGICNUM;
        uint64_t llnum = TTHTONLL(rts);
        memcpy(wp, &llnum, sizeof(llnum));
        wp += sizeof(llnum);
        uint32_t lnum = TTHTONL(rsid);
        memcpy(wp, &lnum, sizeof(lnum));
        wp += sizeof(lnum);
        lnum = TTHTONL(rsiz);
        memcpy(wp, &lnum, sizeof(lnum));
        wp += sizeof(lnum);
        memcpy(wp, rbuf, rsiz);
        if(!ttsocksend(sock, mbuf, msiz)){
          err = true;
          ttservlog(g_serv, TTLOGINFO, "do_repl: response failed");
        }
        pthread_cleanup_pop(1);
      }
    }
    pthread_cleanup_pop(1);
  } else {
    ttservlog(g_serv, TTLOGERROR, "do_repl: tculrdnew failed");
  }
}


/* handle the memcached set command */
static void do_mc_set(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_set command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  if(tnum < 5){
    ttsockprintf(sock, "CLIENT_ERROR error\r\n");
    return;
  }
  bool nr = tnum > 5 && !strcmp(tokens[5], "noreply");
  const char *kbuf = tokens[1];
  int ksiz = strlen(kbuf);
  int vsiz = tclmax(tcatoi(tokens[4]), 0);
  char stack[TTIOBUFSIZ];
  char *vbuf = (vsiz < TTIOBUFSIZ) ? stack : tcmalloc(vsiz + 1);
  pthread_cleanup_push(free, (vbuf == stack) ? NULL : vbuf);
  if(ttsockrecv(sock, vbuf, vsiz) && ttsockgetc(sock) == '\r' && ttsockgetc(sock) == '\n' &&
     !ttsockcheckend(sock)){
    int len;
    if(mask & (TTMSKPUT | TTMSKALLMC | TTMSKALLWRITE)){
      len = sprintf(stack, "CLIENT_ERROR forbidden\r\n");
      ttservlog(g_serv, TTLOGINFO, "do_mc_set: forbidden");
    } else if(tculogadbput(ulog, sid, adb, kbuf, ksiz, vbuf, vsiz)){
      len = sprintf(stack, "STORED\r\n");
    } else {
      len = sprintf(stack, "SERVER_ERROR unexpected\r\n");
      ttservlog(g_serv, TTLOGERROR, "do_mc_set: operation failed");
    }
    if(nr || ttsocksend(sock, stack, len)){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_mc_set: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_set: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the memcached add command */
static void do_mc_add(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_add command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  if(tnum < 5){
    ttsockprintf(sock, "CLIENT_ERROR error\r\n");
    return;
  }
  bool nr = tnum > 5 && !strcmp(tokens[5], "noreply");
  const char *kbuf = tokens[1];
  int ksiz = strlen(kbuf);
  int vsiz = tclmax(tcatoi(tokens[4]), 0);
  char stack[TTIOBUFSIZ];
  char *vbuf = (vsiz < TTIOBUFSIZ) ? stack : tcmalloc(vsiz + 1);
  pthread_cleanup_push(free, (vbuf == stack) ? NULL : vbuf);
  if(ttsockrecv(sock, vbuf, vsiz) && ttsockgetc(sock) == '\r' && ttsockgetc(sock) == '\n' &&
     !ttsockcheckend(sock)){
    int len;
    if(mask & (TTMSKPUTKEEP | TTMSKALLMC | TTMSKALLWRITE)){
      len = sprintf(stack, "CLIENT_ERROR forbidden\r\n");
      ttservlog(g_serv, TTLOGINFO, "do_mc_add: forbidden");
    } else if(tculogadbputkeep(ulog, sid, adb, kbuf, ksiz, vbuf, vsiz)){
      len = sprintf(stack, "STORED\r\n");
    } else {
      len = sprintf(stack, "NOT_STORED\r\n");
    }
    if(nr || ttsocksend(sock, stack, len)){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_mc_add: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_add: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the memcached replace command */
static void do_mc_replace(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_replace command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  if(tnum < 5){
    ttsockprintf(sock, "CLIENT_ERROR error\r\n");
    return;
  }
  bool nr = tnum > 5 && !strcmp(tokens[5], "noreply");
  const char *kbuf = tokens[1];
  int ksiz = strlen(kbuf);
  int vsiz = tclmax(tcatoi(tokens[4]), 0);
  char stack[TTIOBUFSIZ];
  char *vbuf = (vsiz < TTIOBUFSIZ) ? stack : tcmalloc(vsiz + 1);
  pthread_cleanup_push(free, (vbuf == stack) ? NULL : vbuf);
  if(ttsockrecv(sock, vbuf, vsiz) && ttsockgetc(sock) == '\r' && ttsockgetc(sock) == '\n' &&
     !ttsockcheckend(sock)){
    int len;
    if(mask & (TTMSKPUT | TTMSKALLMC | TTMSKALLWRITE)){
      len = sprintf(stack, "CLIENT_ERROR forbidden\r\n");
      ttservlog(g_serv, TTLOGINFO, "do_mc_replace: forbidden");
    } else if(tcadbvsiz(adb, kbuf, ksiz) >= 0 &&
              tculogadbput(ulog, sid, adb, kbuf, ksiz, vbuf, vsiz)){
      len = sprintf(stack, "STORED\r\n");
    } else {
      len = sprintf(stack, "NOT_STORED\r\n");
    }
    if(nr || ttsocksend(sock, stack, len)){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_mc_replace: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_replace: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the memcached get command */
static void do_mc_get(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_get command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  if(tnum < 2){
    ttsockprintf(sock, "CLIENT_ERROR error\r\n");
    return;
  }
  TCXSTR *xstr = tcxstrnew();
  pthread_cleanup_push((void (*)(void *))tcxstrdel, xstr);
  for(int i = 1; i < tnum; i++){
    const char *kbuf = tokens[i];
    int ksiz = strlen(kbuf);
    int vsiz;
    char *vbuf;
    if(mask & (TTMSKGET | TTMSKALLMC | TTMSKALLREAD)){
      vbuf = NULL;
      vsiz = 0;
      ttservlog(g_serv, TTLOGINFO, "do_mc_get: forbidden");
    } else {
      vbuf = tcadbget(adb, kbuf, ksiz, &vsiz);
    }
    if(vbuf){
      tcxstrprintf(xstr, "VALUE %s 0 %d\r\n", kbuf, vsiz);
      tcxstrcat(xstr, vbuf, vsiz);
      tcxstrcat(xstr, "\r\n", 2);
      tcfree(vbuf);
    }
  }
  tcxstrprintf(xstr, "END\r\n");
  if(ttsocksend(sock, tcxstrptr(xstr), tcxstrsize(xstr))){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_get: response failed");
  }
  pthread_cleanup_pop(1);
}


/* handle the memcached delete command */
static void do_mc_delete(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_delete command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  if(tnum < 2){
    ttsockprintf(sock, "CLIENT_ERROR error\r\n");
    return;
  }
  bool nr = (tnum > 2 && !strcmp(tokens[2], "noreply")) ||
    (tnum > 3 && !strcmp(tokens[3], "noreply"));
  const char *kbuf = tokens[1];
  int ksiz = strlen(kbuf);
  char stack[TTIOBUFSIZ];
  int len;
  if(mask & (TTMSKOUT | TTMSKALLMC | TTMSKALLWRITE)){
    len = sprintf(stack, "CLIENT_ERROR forbidden\r\n");
    ttservlog(g_serv, TTLOGINFO, "do_mc_delete: forbidden");
  } else if(tculogadbout(ulog, sid, adb, kbuf, ksiz)){
    len = sprintf(stack, "DELETED\r\n");
  } else {
    len = sprintf(stack, "NOT_FOUND\r\n");
  }
  if(nr || ttsocksend(sock, stack, len)){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_delete: response failed");
  }
}


/* handle the memcached incr command */
static void do_mc_incr(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_incr command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  pthread_mutex_t *rmtxs = arg->rmtxs;
  if(tnum < 3){
    ttsockprintf(sock, "CLIENT_ERROR error\r\n");
    return;
  }
  bool nr = tnum > 3 && !strcmp(tokens[3], "noreply");
  const char *kbuf = tokens[1];
  int ksiz = strlen(kbuf);
  int64_t num = strtoll(tokens[2], NULL, 10);
  int mtxidx = recmtxidx(kbuf, ksiz);
  char stack[TTIOBUFSIZ];
  int len;
  if(mask & (TTMSKADDINT | TTMSKALLMC | TTMSKALLWRITE)){
    len = sprintf(stack, "CLIENT_ERROR forbidden\r\n");
    ttservlog(g_serv, TTLOGINFO, "do_mc_incr: forbidden");
  } else {
    if(pthread_mutex_lock(rmtxs + mtxidx) != 0){
      ttsockprintf(sock, "SERVER_ERROR unexpected\r\n");
      ttservlog(g_serv, TTLOGERROR, "do_mc_incr: pthread_mutex_lock failed");
      return;
    }
    int vsiz;
    char *vbuf = tcadbget(adb, kbuf, ksiz, &vsiz);
    if(vbuf){
      num += strtoll(vbuf, NULL, 10);
      if(num < 0) num = 0;
      len = sprintf(stack, "%lld", (long long)num);
      if(tculogadbput(ulog, sid, adb, kbuf, ksiz, stack, len)){
        len = sprintf(stack, "%lld\r\n", (long long)num);
      } else {
        len = sprintf(stack, "SERVER_ERROR unexpected\r\n");
        ttservlog(g_serv, TTLOGERROR, "do_mc_incr: operation failed");
      }
      tcfree(vbuf);
    } else {
      len = sprintf(stack, "NOT_FOUND\r\n");
    }
    if(pthread_mutex_unlock(rmtxs + mtxidx) != 0)
      ttservlog(g_serv, TTLOGERROR, "do_mc_incr: pthread_mutex_unlock failed");
  }
  if(nr || ttsocksend(sock, stack, len)){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_incr: response failed");
  }
}


/* handle the memcached decr command */
static void do_mc_decr(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_decr command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  pthread_mutex_t *rmtxs = arg->rmtxs;
  if(tnum < 3){
    ttsockprintf(sock, "CLIENT_ERROR error\r\n");
    return;
  }
  bool nr = tnum > 3 && !strcmp(tokens[3], "noreply");
  const char *kbuf = tokens[1];
  int ksiz = strlen(kbuf);
  int64_t num = strtoll(tokens[2], NULL, 10) * -1;
  int mtxidx = recmtxidx(kbuf, ksiz);
  char stack[TTIOBUFSIZ];
  int len;
  if(mask & (TTMSKADDINT | TTMSKALLMC | TTMSKALLWRITE)){
    len = sprintf(stack, "CLIENT_ERROR forbidden\r\n");
    ttservlog(g_serv, TTLOGINFO, "do_mc_decr: forbidden");
  } else {
    if(pthread_mutex_lock(rmtxs + mtxidx) != 0){
      ttsockprintf(sock, "SERVER_ERROR unexpected\r\n");
      ttservlog(g_serv, TTLOGERROR, "do_mc_decr: pthread_mutex_lock failed");
      return;
    }
    int vsiz;
    char *vbuf = tcadbget(adb, kbuf, ksiz, &vsiz);
    if(vbuf){
      num += strtoll(vbuf, NULL, 10);
      if(num < 0) num = 0;
      len = sprintf(stack, "%lld", (long long)num);
      if(tculogadbput(ulog, sid, adb, kbuf, ksiz, stack, len)){
        len = sprintf(stack, "%lld\r\n", (long long)num);
      } else {
        len = sprintf(stack, "SERVER_ERROR unexpected\r\n");
        ttservlog(g_serv, TTLOGERROR, "do_mc_decr: operation failed");
      }
      tcfree(vbuf);
    } else {
      len = sprintf(stack, "NOT_FOUND\r\n");
    }
    if(pthread_mutex_unlock(rmtxs + mtxidx) != 0)
      ttservlog(g_serv, TTLOGERROR, "do_mc_decr: pthread_mutex_unlock failed");
  }
  if(nr || ttsocksend(sock, stack, len)){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_decr: response failed");
  }
}


/* handle the memcached stat command */
static void do_mc_stats(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_stats command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  char stack[TTIOBUFSIZ];
  char *wp = stack;
  if(mask & (TTMSKSTAT | TTMSKALLMC | TTMSKALLREAD)){
    ttservlog(g_serv, TTLOGINFO, "do_mc_stats: forbidden");
  } else {
    wp += sprintf(wp, "STAT pid %lld\r\n", (long long)getpid());
    time_t now = time(NULL);
    wp += sprintf(wp, "STAT uptime %lld\r\n", (long long)(now - (int)g_starttime));
    wp += sprintf(wp, "STAT time %lld\r\n", (long long)now);
    wp += sprintf(wp, "STAT version %s\r\n", ttversion);
    struct rusage ubuf;
    memset(&ubuf, 0, sizeof(ubuf));
    if(getrusage(RUSAGE_SELF, &ubuf) == 0){
      wp += sprintf(wp, "STAT rusage_user %d.%06d\r\n",
                    (int)ubuf.ru_utime.tv_sec, (int)ubuf.ru_utime.tv_usec);
      wp += sprintf(wp, "STAT rusage_system %d.%06d\r\n",
                    (int)ubuf.ru_stime.tv_sec, (int)ubuf.ru_stime.tv_usec);
    }
    wp += sprintf(wp, "STAT curr_items %lld\r\n", (long long)tcadbrnum(adb));
    wp += sprintf(wp, "STAT bytes %lld\r\n", (long long)tcadbsize(adb));
    wp += sprintf(wp, "END\r\n");
  }
  if(ttsocksend(sock, stack, wp - stack)){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_stats: response failed");
  }
}


/* handle the memcached flush_all command */
static void do_mc_flushall(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_flushall command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  bool nr = (tnum > 1 && !strcmp(tokens[1], "noreply")) ||
    (tnum > 2 && !strcmp(tokens[2], "noreply"));
  char stack[TTIOBUFSIZ];
  int len;
  if(mask & (TTMSKVANISH | TTMSKALLMC | TTMSKALLWRITE)){
    len = sprintf(stack, "CLIENT_ERROR forbidden\r\n");
    ttservlog(g_serv, TTLOGINFO, "do_mc_flushall: forbidden");
  } else if(tculogadbvanish(ulog, sid, adb)){
    len = sprintf(stack, "OK\r\n");
  } else {
    len = sprintf(stack, "SERVER_ERROR unexpected\r\n");
    ttservlog(g_serv, TTLOGERROR, "do_mc_flushall: operation failed");
  }
  if(nr || ttsocksend(sock, stack, len)){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_flushall: response failed");
  }
}


/* handle the memcached version command */
static void do_mc_version(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_version command");
  uint64_t mask = arg->mask;
  char stack[TTIOBUFSIZ];
  int len;
  if(mask & (TTMSKSTAT | TTMSKALLMC | TTMSKALLREAD)){
    len = sprintf(stack, "CLIENT_ERROR forbidden\r\n");
    ttservlog(g_serv, TTLOGINFO, "do_mc_version: forbidden");
  } else {
    len = sprintf(stack, "VERSION %s\r\n", ttversion);
  }
  if(ttsocksend(sock, stack, len)){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_version: response failed");
  }
}


/* handle the memcached quit command */
static void do_mc_quit(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_quit command");
}


/* handle the HTTP GET command */
static void do_http_get(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri){
  ttservlog(g_serv, TTLOGDEBUG, "doing http_get command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  bool keep = ver >= 1;
  char line[LINEBUFSIZ];
  while(ttsockgets(sock, line, LINEBUFSIZ) && *line != '\0'){
    char *pv = strchr(line, ':');
    if(!pv) continue;
    *(pv++) = '\0';
    while(*pv == ' ' || *pv == '\t'){
      pv++;
    }
    if(!tcstricmp(line, "connection")){
      if(!tcstricmp(pv, "close")){
        keep = false;
      } else if(!tcstricmp(pv, "keep-alive")){
        keep = true;
      }
    }
  }
  if(*uri == '/') uri++;
  int ksiz;
  char *kbuf = tcurldecode(uri, &ksiz);
  pthread_cleanup_push(free, kbuf);
  TCXSTR *xstr = tcxstrnew();
  pthread_cleanup_push((void (*)(void *))tcxstrdel, xstr);
  if(mask & (TTMSKGET | TTMSKALLHTTP | TTMSKALLREAD)){
    int len = sprintf(line, "Forbidden\n");
    tcxstrprintf(xstr, "HTTP/1.1 403 Forbidden\r\n");
    tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
    tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
    tcxstrprintf(xstr, "\r\n");
    tcxstrcat(xstr, line, len);
    ttservlog(g_serv, TTLOGINFO, "do_http_get: forbidden");
  } else {
    int vsiz;
    char *vbuf = tcadbget(adb, kbuf, ksiz, &vsiz);
    if(vbuf){
      tcxstrprintf(xstr, "HTTP/1.1 200 OK\r\n");
      tcxstrprintf(xstr, "Content-Type: application/octet-stream\r\n");
      tcxstrprintf(xstr, "Content-Length: %d\r\n", vsiz);
      tcxstrprintf(xstr, "\r\n");
      tcxstrcat(xstr, vbuf, vsiz);
      tcfree(vbuf);
    } else {
      int len = sprintf(line, "Not Found\n");
      tcxstrprintf(xstr, "HTTP/1.1 404 Not Found\r\n");
      tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
      tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
      tcxstrprintf(xstr, "\r\n");
      tcxstrcat(xstr, line, len);
    }
  }
  if(ttsocksend(sock, tcxstrptr(xstr), tcxstrsize(xstr))){
    req->keep = keep;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_http_get: response failed");
  }
  pthread_cleanup_pop(1);
  pthread_cleanup_pop(1);
}


/* handle the HTTP HEAD command */
static void do_http_head(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri){
  ttservlog(g_serv, TTLOGDEBUG, "doing http_head command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  bool keep = ver >= 1;
  char line[LINEBUFSIZ];
  while(ttsockgets(sock, line, LINEBUFSIZ) && *line != '\0'){
    char *pv = strchr(line, ':');
    if(!pv) continue;
    *(pv++) = '\0';
    while(*pv == ' ' || *pv == '\t'){
      pv++;
    }
    if(!tcstricmp(line, "connection")){
      if(!tcstricmp(pv, "close")){
        keep = false;
      } else if(!tcstricmp(pv, "keep-alive")){
        keep = true;
      }
    }
  }
  if(*uri == '/') uri++;
  int ksiz;
  char *kbuf = tcurldecode(uri, &ksiz);
  pthread_cleanup_push(free, kbuf);
  TCXSTR *xstr = tcxstrnew();
  pthread_cleanup_push((void (*)(void *))tcxstrdel, xstr);
  if(mask & (TTMSKVSIZ | TTMSKALLHTTP | TTMSKALLREAD)){
    tcxstrprintf(xstr, "HTTP/1.1 403 Forbidden\r\n");
    tcxstrprintf(xstr, "\r\n");
    ttservlog(g_serv, TTLOGINFO, "do_http_head: forbidden");
  } else {
    int vsiz = tcadbvsiz(adb, kbuf, ksiz);
    if(vsiz >= 0){
      tcxstrprintf(xstr, "HTTP/1.1 200 OK\r\n");
      tcxstrprintf(xstr, "Content-Type: application/octet-stream\r\n");
      tcxstrprintf(xstr, "Content-Length: %d\r\n", vsiz);
      tcxstrprintf(xstr, "\r\n");
    } else {
      tcxstrprintf(xstr, "HTTP/1.1 404 Not Found\r\n");
      tcxstrprintf(xstr, "\r\n");
    }
  }
  if(ttsocksend(sock, tcxstrptr(xstr), tcxstrsize(xstr))){
    req->keep = keep;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_http_head: response failed");
  }
  pthread_cleanup_pop(1);
  pthread_cleanup_pop(1);
}


/* handle the HTTP PUT command */
static void do_http_put(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri){
  ttservlog(g_serv, TTLOGDEBUG, "doing http_put command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  bool keep = ver >= 1;
  int vsiz = 0;
  int pdmode = 0;
  char line[LINEBUFSIZ];
  while(ttsockgets(sock, line, LINEBUFSIZ) && *line != '\0'){
    char *pv = strchr(line, ':');
    if(!pv) continue;
    *(pv++) = '\0';
    while(*pv == ' ' || *pv == '\t'){
      pv++;
    }
    if(!tcstricmp(line, "connection")){
      if(!tcstricmp(pv, "close")){
        keep = false;
      } else if(!tcstricmp(pv, "keep-alive")){
        keep = true;
      }
    } else if(!tcstricmp(line, "content-length")){
      vsiz = tcatoi(pv);
    } else if(!tcstricmp(line, "x-tt-pdmode")){
      pdmode = tcatoi(pv);
    }
  }
  if(*uri == '/') uri++;
  int ksiz;
  char *kbuf = tcurldecode(uri, &ksiz);
  pthread_cleanup_push(free, kbuf);
  char stack[TTIOBUFSIZ];
  char *vbuf = (vsiz < TTIOBUFSIZ) ? stack : tcmalloc(vsiz + 1);
  pthread_cleanup_push(free, (vbuf == stack) ? NULL : vbuf);
  if(vsiz >= 0 && ttsockrecv(sock, vbuf, vsiz) && !ttsockcheckend(sock)){
    TCXSTR *xstr = tcxstrnew();
    pthread_cleanup_push((void (*)(void *))tcxstrdel, xstr);
    if(mask & (TTMSKPUT | TTMSKALLHTTP | TTMSKALLWRITE)){
      int len = sprintf(line, "Forbidden\n");
      tcxstrprintf(xstr, "HTTP/1.1 403 Forbidden\r\n");
      tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
      tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
      tcxstrprintf(xstr, "\r\n");
      tcxstrcat(xstr, line, len);
      ttservlog(g_serv, TTLOGINFO, "do_http_put: forbidden");
    } else {
      switch(pdmode){
      case 1:
        if(tculogadbputkeep(ulog, sid, adb, kbuf, ksiz, vbuf, vsiz)){
          int len = sprintf(line, "Created\n");
          tcxstrprintf(xstr, "HTTP/1.1 201 Created\r\n");
          tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
          tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
          tcxstrprintf(xstr, "\r\n");
          tcxstrcat(xstr, line, len);
        } else {
          int len = sprintf(line, "Conflict\n");
          tcxstrprintf(xstr, "HTTP/1.1 409 Conflict\r\n");
          tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
          tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
          tcxstrprintf(xstr, "\r\n");
          tcxstrcat(xstr, line, len);
        }
        break;
      case 2:
        if(tculogadbputcat(ulog, sid, adb, kbuf, ksiz, vbuf, vsiz)){
          int len = sprintf(line, "Created\n");
          tcxstrprintf(xstr, "HTTP/1.1 201 Created\r\n");
          tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
          tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
          tcxstrprintf(xstr, "\r\n");
          tcxstrcat(xstr, line, len);
        } else {
          int len = sprintf(line, "Internal Server Error\n");
          tcxstrprintf(xstr, "HTTP/1.1 500 Internal Server Error\r\n");
          tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
          tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
          tcxstrprintf(xstr, "\r\n");
          tcxstrcat(xstr, line, len);
          ttservlog(g_serv, TTLOGERROR, "do_http_put: operation failed");
        }
        break;
      default:
        if(tculogadbput(ulog, sid, adb, kbuf, ksiz, vbuf, vsiz)){
          int len = sprintf(line, "Created\n");
          tcxstrprintf(xstr, "HTTP/1.1 201 Created\r\n");
          tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
          tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
          tcxstrprintf(xstr, "\r\n");
          tcxstrcat(xstr, line, len);
        } else {
          int len = sprintf(line, "Internal Server Error\n");
          tcxstrprintf(xstr, "HTTP/1.1 500 Internal Server Error\r\n");
          tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
          tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
          tcxstrprintf(xstr, "\r\n");
          tcxstrcat(xstr, line, len);
          ttservlog(g_serv, TTLOGERROR, "do_http_put: operation failed");
        }
      }
    }
    if(ttsocksend(sock, tcxstrptr(xstr), tcxstrsize(xstr))){
      req->keep = keep;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_http_put: response failed");
    }
    pthread_cleanup_pop(1);
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_http_put: invalid entity");
  }
  pthread_cleanup_pop(1);
  pthread_cleanup_pop(1);
}


/* handle the HTTP POST command */
static void do_http_post(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri){
  ttservlog(g_serv, TTLOGDEBUG, "doing http_post command");
  uint64_t mask = arg->mask;
  pthread_mutex_t *rmtxs = arg->rmtxs;
  void *scr = arg->screxts[req->idx];
  bool keep = ver >= 1;
  int vsiz = 0;
  char name[LINEBUFSIZ/4+1];
  *name = '\0';
  int opts = 0;
  char line[LINEBUFSIZ];
  while(ttsockgets(sock, line, LINEBUFSIZ) && *line != '\0'){
    char *pv = strchr(line, ':');
    if(!pv) continue;
    *(pv++) = '\0';
    while(*pv == ' ' || *pv == '\t'){
      pv++;
    }
    if(!tcstricmp(line, "connection")){
      if(!tcstricmp(pv, "close")){
        keep = false;
      } else if(!tcstricmp(pv, "keep-alive")){
        keep = true;
      }
    } else if(!tcstricmp(line, "content-length")){
      vsiz = tcatoi(pv);
    } else if(!tcstricmp(line, "x-tt-xname")){
      snprintf(name, sizeof(name), "%s", pv);
      name[sizeof(name)-1] = '\0';
    } else if(!tcstricmp(line, "x-tt-xopts")){
      opts = tcatoi(pv);
    }
  }
  if(*uri == '/') uri++;
  int ksiz;
  char *kbuf = tcurldecode(uri, &ksiz);
  pthread_cleanup_push(free, kbuf);
  char stack[TTIOBUFSIZ];
  char *vbuf = (vsiz < TTIOBUFSIZ) ? stack : tcmalloc(vsiz + 1);
  pthread_cleanup_push(free, (vbuf == stack) ? NULL : vbuf);
  if(vsiz >= 0 && ttsockrecv(sock, vbuf, vsiz) && !ttsockcheckend(sock)){
    TCXSTR *xstr = tcxstrnew();
    pthread_cleanup_push((void (*)(void *))tcxstrdel, xstr);
    if(mask & (TTMSKEXT | TTMSKALLHTTP)){
      int len = sprintf(line, "Forbidden\n");
      tcxstrprintf(xstr, "HTTP/1.1 403 Forbidden\r\n");
      tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
      tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
      tcxstrprintf(xstr, "\r\n");
      tcxstrcat(xstr, line, len);
      ttservlog(g_serv, TTLOGINFO, "do_http_post: forbidden");
    } else {
      int xsiz = 0;
      char *xbuf = NULL;
      if(scr){
        if(opts & RDBXOLCKGLB){
          bool err = false;
          for(int i = 0; i < RECMTXNUM; i++){
            if(pthread_mutex_lock(rmtxs + i) != 0){
              ttservlog(g_serv, TTLOGERROR, "do_http_post: pthread_mutex_lock failed");
              while(--i >= 0){
                pthread_mutex_unlock(rmtxs + i);
              }
              err = true;
              break;
            }
          }
          if(!err){
            xbuf = scrextcallmethod(scr, name, kbuf, ksiz, vbuf, vsiz, &xsiz);
            for(int i = RECMTXNUM - 1; i >= 0; i--){
              if(pthread_mutex_unlock(rmtxs + i) != 0)
                ttservlog(g_serv, TTLOGERROR, "do_http_post: pthread_mutex_unlock failed");
            }
          }
        } else if(opts & RDBXOLCKREC){
          int mtxidx = recmtxidx(kbuf, ksiz);
          if(pthread_mutex_lock(rmtxs + mtxidx) == 0){
            xbuf = scrextcallmethod(scr, name, kbuf, ksiz, vbuf, vsiz, &xsiz);
            if(pthread_mutex_unlock(rmtxs + mtxidx) != 0)
              ttservlog(g_serv, TTLOGERROR, "do_http_post: pthread_mutex_unlock failed");
          } else {
            ttservlog(g_serv, TTLOGERROR, "do_http_post: pthread_mutex_lock failed");
          }
        } else {
          xbuf = scrextcallmethod(scr, name, kbuf, ksiz, vbuf, vsiz, &xsiz);
        }
      }
      if(xbuf){
        tcxstrprintf(xstr, "HTTP/1.1 200 OK\r\n");
        tcxstrprintf(xstr, "Content-Type: application/octet-stream\r\n");
        tcxstrprintf(xstr, "Content-Length: %d\r\n", xsiz);
        tcxstrprintf(xstr, "\r\n");
        tcxstrcat(xstr, xbuf, xsiz);
        tcfree(xbuf);
      } else {
        int len = sprintf(line, "Internal Server Error");
        tcxstrprintf(xstr, "HTTP/1.1 500 Internal Server Error\r\n");
        tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
        tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
        tcxstrprintf(xstr, "\r\n");
        tcxstrcat(xstr, line, len);
      }
    }
    if(ttsocksend(sock, tcxstrptr(xstr), tcxstrsize(xstr))){
      req->keep = keep;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_http_post: response failed");
    }
    pthread_cleanup_pop(1);
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_http_post: invalid entity");
  }
  pthread_cleanup_pop(1);
  pthread_cleanup_pop(1);
}


/* handle the HTTP DELETE command */
static void do_http_delete(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri){
  ttservlog(g_serv, TTLOGDEBUG, "doing http_delete command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  bool keep = ver >= 1;
  char line[LINEBUFSIZ];
  while(ttsockgets(sock, line, LINEBUFSIZ) && *line != '\0'){
    char *pv = strchr(line, ':');
    if(!pv) continue;
    *(pv++) = '\0';
    while(*pv == ' ' || *pv == '\t'){
      pv++;
    }
    if(!tcstricmp(line, "connection")){
      if(!tcstricmp(pv, "close")){
        keep = false;
      } else if(!tcstricmp(pv, "keep-alive")){
        keep = true;
      }
    }
  }
  if(*uri == '/') uri++;
  int ksiz;
  char *kbuf = tcurldecode(uri, &ksiz);
  pthread_cleanup_push(free, kbuf);
  TCXSTR *xstr = tcxstrnew();
  pthread_cleanup_push((void (*)(void *))tcxstrdel, xstr);
  if(mask & (TTMSKOUT | TTMSKALLHTTP | TTMSKALLWRITE)){
    int len = sprintf(line, "Forbidden\n");
    tcxstrprintf(xstr, "HTTP/1.1 403 Forbidden\r\n");
    tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
    tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
    tcxstrprintf(xstr, "\r\n");
    tcxstrcat(xstr, line, len);
    ttservlog(g_serv, TTLOGINFO, "do_http_delete: forbidden");
  } else {
    if(tculogadbout(ulog, sid, adb, kbuf, ksiz)){
      int len = sprintf(line, "OK\n");
      tcxstrprintf(xstr, "HTTP/1.1 200 OK\r\n");
      tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
      tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
      tcxstrprintf(xstr, "\r\n");
      tcxstrcat(xstr, line, len);
    } else {
      int len = sprintf(line, "Not Found\n");
      tcxstrprintf(xstr, "HTTP/1.1 404 Not Found\r\n");
      tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
      tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
      tcxstrprintf(xstr, "\r\n");
      tcxstrcat(xstr, line, len);
    }
  }
  if(ttsocksend(sock, tcxstrptr(xstr), tcxstrsize(xstr))){
    req->keep = keep;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_http_delete: response failed");
  }
  pthread_cleanup_pop(1);
  pthread_cleanup_pop(1);
}



// END OF FILE
