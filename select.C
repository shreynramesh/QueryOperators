#include "catalog.h"
#include "query.h"
#include "stdio.h"
#include "stdlib.h"

// forward declaration
const Status ScanSelect(const string &result,
                        const int projCnt,
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc,
                        const Operator op,
                        const char *filter,
                        const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string &result,
                       const int projCnt,
                       const attrInfo projNames[],
                       const attrInfo *attr,
                       const Operator op,
                       const char *attrValue) {
    Status status;
    AttrDesc attrDescArray[projCnt];
    AttrDesc *attrDesc = NULL;
    int reclen = 0;

    // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;

    // go through the projection list and look up each in the
    // attr cat to get an AttrDesc structure (for offset, length, etc)

    for (int i = 0; i < projCnt; i++) {
        Status status = attrCat->getInfo(projNames[i].relName,
                                         projNames[i].attrName,
                                         attrDescArray[i]);
        if (status != OK) {
            return status;
        }
        // reclen += attrDescArray[i].attrLen;
    }

    // get AttrDesc structure for the select attribute
    if (attr != NULL) {
        attrDesc = new AttrDesc;
        status = attrCat->getInfo(attr->relName, attr->attrName, *attrDesc);
        if (status != OK) {
            return status;
        }
    }

    for (int i = 0; i < projCnt; i++) {
        reclen += attrDescArray[i].attrLen;
    }

    return ScanSelect(result, projCnt, attrDescArray, attrDesc, op, attrValue, reclen);
}

const Status ScanSelect(const string &result,
                        const int projCnt,
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc,
                        const Operator op,
                        const char *filter,
                        const int reclen) {
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

    Status status;
    Record tmpRec;
    Record outRec;
    RID tmpRid;
    // RID outRID;

    // Opening resulting relation
    InsertFileScan resRel(result, status);
    if (status != OK) {
        return status;
    }

    // Opening current table
    HeapFileScan scanRel(projNames[0].relName, status);
    if (status != OK) {
        return status;
    }

    // Checking if unconditional scan is required
    if (attrDesc == NULL) {
        status = scanRel.startScan(0, 0, STRING, NULL, EQ);
    } else {
        switch (attrDesc->attrType) {
            case STRING: {
                status = scanRel.startScan(attrDesc->attrOffset, attrDesc->attrLen, STRING, filter, op);
                break;
            }
            case INTEGER: {
                int tmpInt = atoi(filter);
                status = scanRel.startScan(attrDesc->attrOffset, attrDesc->attrLen, INTEGER, (char *)&tmpInt, op);
                break;
            }
            case FLOAT: {
                float tmpFloat = atof(filter);
                status = scanRel.startScan(attrDesc->attrOffset, attrDesc->attrLen, FLOAT, (char *)&tmpFloat, op);
                break;
            }
        }
    }
    if (status != OK) {
        return status;
    }

    // Setting up outrec
    outRec.length = reclen;
    cout << "here";
    // Scanning relation
    while ((status = scanRel.scanNext(tmpRid)) == OK) {
        status = scanRel.getRecord(tmpRec);
        if (status != OK) {
            return status;
        }

        // Looping through specified projections to make output record
        int outRecOffset = 0;
        for (int i = 0; i < projCnt; i++) {
            memcpy((char *)outRec.data + outRecOffset, (char *)tmpRec.data + projNames[i].attrOffset, projNames[i].attrLen);
            outRecOffset += projNames[i].attrLen;
        }

        RID outRID;
        status = resRel.insertRecord(outRec, outRID);
    }

    // Checking if there was something wrong with the scan - should have reached EOF
    if (status != FILEEOF) {
        return status;
    }

    // Ending scan
    status = scanRel.endScan();
    if (status != OK) {
        return status;
    }

    return OK;
}
