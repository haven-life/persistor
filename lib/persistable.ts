export class Persistor extends ObjectTemplate {
    /**
     * @TODO: was typed `Persistor` but that's weird? doesn't work need to figure out what's going on.
     */
    static create(): any | undefined {return undefined}; 
    endTransaction(persistorTransaction?, logger?) : any {}
    begin (isdefault?) : any {}
    end (persistorTransaction?, logger?) : any {};
    setDirty (obj, txn?, onlyIfChanged?, noCascade?, logger?) {};
    setAsDeleted (obj, txn?, onlyIfChanged?) {};
    saveAll (txn?, logger?) : any {return undefined};
    setDB(db, type, alias) {};
    getPOJOFromQuery (template, query, options?, logger?) : any {}
    commit (options?) : any {};

    getPersistorProps () : any {}

    connect (connect : any, schema : any) : any {}
    dropAllTables () : any {}
    syncAllTables () : any {}
    onAllTables (callback : Function, concurrency? : number) : any {}

    debugInfo : any
    DB_Knex : any;

    countFromKnexQuery (template, queryOrChains, _logger?) : any {}
    dropKnexTable (template : string) : any {};
    synchronizeKnexTableFromTemplate (template : string, changeNotificationCallback? : any, forceSync? : boolean) : any {};
    setSchema(schema : any) {};
    appendSchema(schema : any) {};
    performInjections() {}
    config: any
    __transient__ : any
    objectMap: any
    static createTransientObject(callback : any) : any {};
}