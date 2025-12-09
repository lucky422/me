/* kytesql.c — Public-domain SQLite parser + KyteDB backend */
/* Compile with the 5 PD files + your kytedb.c
 *
 *   gcc -O3 kytesql.c parse.c tokenize.c vdbe.c opcode.c where.c 
 *       kytedb.c -o kytesql
 */
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>      // calloc, malloc, free
#include "sqlite3.h"     // PD structs (Parse, Vdbe, etc.)
#include "kytedb.h"      // Your KV (assume Entry, KDB, meta->root, map, mapsize)
#define TABLE_NAME  "kv"
#define MODULE_NAME "kytedb"
#define KEY_COL     "key"
#define VAL_COL     "value"
static KDB *g_db = NULL;
/* ------------------------------------------------------------------
   Virtual table module for KyteDB
   ------------------------------------------------------------------ */
/* Cursor struct – tracks current offset for scans */
typedef struct {
    sqlite3_vtab_cursor base;
    uint64_t off;  // current chain offset (byte offset into db->map)
} KyteCursor;
/* vtab struct – dummy */
typedef struct {
    sqlite3_vtab base;
} KyteVtab;
/* xConnect: attach vtab and declare schema */
static int kyteConnect(sqlite3 *handle, void *pAux, int argc,
                       const char *const *argv,
                       sqlite3_vtab **ppVtab, char **pzErr)
{
    (void)pAux;
    (void)argc;
    (void)argv;
    (void)pzErr;
    KyteVtab *tab = (KyteVtab *)calloc(1, sizeof(KyteVtab));
    if (!tab) return SQLITE_NOMEM;
    *ppVtab = (sqlite3_vtab *)tab;
    /* Single table: kv(key TEXT PRIMARY KEY, value BLOB) */
    int rc = sqlite3_declare_vtab(
        handle,
        "CREATE TABLE kv(key TEXT PRIMARY KEY, value BLOB)"
    );
    return rc;
}
/* xDisconnect: release vtab object */
static int kyteDisconnect(sqlite3_vtab *pVtab)
{
    free(pVtab);
    return SQLITE_OK;
}
/* xBestIndex: trivial – always full scan */
static int kyteBestIndex(sqlite3_vtab *pVTab, sqlite3_index_info *pIdxInfo)
{
    (void)pVTab;
    /* We do not use any constraints yet – always full scan */
    pIdxInfo->idxNum = 0;
    pIdxInfo->estimatedCost = 1e9;      // arbitrary large
    pIdxInfo->estimatedRows = 1000000;  // arbitrary
    return SQLITE_OK;
}
/* xOpen: allocate a cursor and position at root */
static int kyteOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor)
{
    (void)pVtab;
    KyteCursor *cur = (KyteCursor *)calloc(1, sizeof(KyteCursor));
    if (!cur) return SQLITE_NOMEM;
    /* Start at root of your KyteDB chain */
    cur->off = g_db ? g_db->meta->root : 0;
    *ppCursor = (sqlite3_vtab_cursor *)cur;
    return SQLITE_OK;
}
/* xClose: free cursor */
static int kyteClose(sqlite3_vtab_cursor *pCursor)
{
    free(pCursor);
    return SQLITE_OK;
}
/* xFilter: (re)start scan from root; ignore WHERE for now */
static int kyteFilter(sqlite3_vtab_cursor *pCursor, int idxNum,
                      const char *idxStr, int argc, sqlite3_value **argv)
{
    (void)idxNum;
    (void)idxStr;
    (void)argc;
    (void)argv;
    KyteCursor *cur = (KyteCursor *)pCursor;
    if (g_db) {
        cur->off = g_db->meta->root;
    } else {
        cur->off = 0;
    }
    return SQLITE_OK;
}
/* xNext: advance to the next entry in the chain */
static int kyteNext(sqlite3_vtab_cursor *pCursor)
{
    KyteCursor *cur = (KyteCursor *)pCursor;
    if (!g_db) {
        cur->off = 0;
        return SQLITE_OK;
    }
    if (cur->off && cur->off < g_db->mapsize) {
        Entry *e = (Entry *)(g_db->map + cur->off);
        cur->off = e->next;  // move to next entry in your chain
    } else {
        cur->off = 0;
    }
    return SQLITE_OK;
}
/* xEof: true when offset is zero or past mapsize */
static int kyteEof(sqlite3_vtab_cursor *pCursor)
{
    KyteCursor *cur = (KyteCursor *)pCursor;
    if (!g_db) return 1;
    return (cur->off == 0 || cur->off >= g_db->mapsize);
}
/* xColumn: read key or value from current Entry */
static int kyteColumn(sqlite3_vtab_cursor *pCursor,
                      sqlite3_context *ctx, int col)
{
    KyteCursor *cur = (KyteCursor *)pCursor;
    if (!g_db || !cur->off || cur->off >= g_db->mapsize) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }
    Entry *e = (Entry *)(g_db->map + cur->off);
    uint8_t *k = (uint8_t *)e + sizeof(Entry);
    uint8_t *v = k + e->klen;
    if (col == 0) {
        /* key column */
        sqlite3_result_text(ctx, (char *)k, (int)e->klen, SQLITE_TRANSIENT);
    } else if (col == 1) {
        /* value column */
        sqlite3_result_blob(ctx, v, (int)e->vlen, SQLITE_TRANSIENT);
    } else {
        sqlite3_result_null(ctx);
    }
    return SQLITE_OK;
}
/* xRowid: we just use the offset as a unique rowid */
static int kyteRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid)
{
    KyteCursor *cur = (KyteCursor *)pCursor;
    *pRowid = (sqlite3_int64)cur->off;
    return SQLITE_OK;
}
/* Read-only module: no xUpdate/transactions */
static sqlite3_module kyteModule = {
    0,                       /* iVersion */
    0,                       /* xCreate (we only support xConnect) */
    kyteConnect,             /* xConnect */
    kyteBestIndex,           /* xBestIndex */
    kyteDisconnect,          /* xDisconnect */
    0,                       /* xDestroy */
    kyteOpen,                /* xOpen */
    kyteClose,               /* xClose */
    kyteFilter,              /* xFilter */
    kyteNext,                /* xNext */
    kyteEof,                 /* xEof */
    kyteColumn,              /* xColumn */
    kyteRowid,               /* xRowid */
    0,                       /* xUpdate (read-only) */
    0,                       /* xBegin */
    0,                       /* xSync */
    0,                       /* xCommit */
    0,                       /* xRollback */
    0,                       /* xFindFunction */
    0,                       /* xRename */
    0,                       /* xSavepoint */
    0,                       /* xRelease */
    0                        /* xRollbackTo */
};
/* ------------------------------------------------------------------
   Public API – exec SQL on your KV
   ------------------------------------------------------------------ */
int kytesql_exec(KDB *db, const char *sql,
                 int (*callback)(void*,int,char**,char**), void *arg)
{
    sqlite3 *handle = NULL;
    sqlite3_stmt *stmt = NULL;
    int rc;
    g_db = db;
    rc = sqlite3_initialize();
    if (rc != SQLITE_OK) return rc;
    rc = sqlite3_open(":memory:", &handle);
    if (rc != SQLITE_OK) goto end;
    /* Register module under MODULE_NAME (e.g., "kytedb") */
    rc = sqlite3_create_module(handle, MODULE_NAME, &kyteModule, NULL);
    if (rc != SQLITE_OK) goto end;
    /* CREATE VIRTUAL TABLE kv USING kytedb(); */
    char create[256];
    snprintf(create, sizeof(create),
             "CREATE VIRTUAL TABLE %s USING %s()",
             TABLE_NAME, MODULE_NAME);
    rc = sqlite3_exec(handle, create, NULL, NULL, NULL);
    if (rc != SQLITE_OK) goto end;
    rc = sqlite3_prepare_v2(handle, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) goto end;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        int cols = sqlite3_column_count(stmt);
        char **values = (char **)malloc(cols * sizeof(char *));
        char **names  = (char **)malloc(cols * sizeof(char *));
        if (!values || !names) {
            rc = SQLITE_NOMEM;
            break;
        }
        for (int i = 0; i < cols; i++) {
            const char *txt = (const char *)sqlite3_column_text(stmt, i);
            values[i] = txt ? strdup(txt) : NULL;
            names[i]  = strdup(sqlite3_column_name(stmt, i));
        }
        if (callback(arg, cols, values, names)) {
            /* user requested stop */
            for (int i = 0; i < cols; i++) {
                free(values[i]);
                free(names[i]);
            }
            free(values);
            free(names);
            break;
        }
        for (int i = 0; i < cols; i++) {
            free(values[i]);
            free(names[i]);
        }
        free(values);
        free(names);
    }
end:
    if (stmt)   sqlite3_finalize(stmt);
    if (handle) sqlite3_close(handle);
    sqlite3_shutdown();
    /* Normalize rc: treat SQLITE_DONE as success */
    return (rc == SQLITE_DONE || rc == SQLITE_ROW) ? SQLITE_OK : rc;
}
/* ------------------------------------------------------------------
   Demo main
   ------------------------------------------------------------------ */
static int print_row(void *unused, int cols, char **values, char **names)
{
    (void)unused;
    for (int i = 0; i < cols; i++) {
        printf("%s = %s  ", names[i], values[i] ? values[i] : "NULL");
    }
    printf("\n");
    return 0;
}
int main(void)
{
    KDB *db = NULL;
    /* Open your KyteDB file */
    if (kytedb_open(&db, "test.kdb", (uint64_t)1 << 26) != 0) {
        fprintf(stderr, "kytedb_open failed\n");
        return 1;
    }
    /* Pretend we have some data */
    kytedb_put(db, (uint8_t *)"name", 4, (uint8_t *)"Alice", 5);
    kytedb_put(db, (uint8_t *)"age",  3, (uint8_t *)"31",    2);
    kytedb_put(db, (uint8_t *)"city", 4, (uint8_t *)"NYC",   3);
    /* Run a SQL query over the kv table */
    int rc = kytesql_exec(
        db,
        "SELECT key, value FROM kv "
        "WHERE key LIKE 'a%' "
        "ORDER BY key "
        "LIMIT 2",
        print_row,
        NULL
    );
    if (rc != SQLITE_OK) {
        fprintf(stderr, "kytesql_exec error: %d\n", rc);
    }
    kytedb_close(db);
    return 0;
}
