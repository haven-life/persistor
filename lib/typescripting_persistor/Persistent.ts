// Should default to Supertype's Constructor

import { Supertype } from 'supertype';
import { PersistObjectTemplate } from './PersistObjectTemplate';
import { UtilityFunctions } from './UtilityFunctions';
import { ObjectID } from 'mongodb';
import { SchemaValidator } from './SchemaValidator';
import { Mongo } from './Mongo';


export type Constructor<T> = new(...args) => T;
export type PersistentConstructor = typeof Persistent;

export class Persistent extends Supertype {

    __template__: typeof Persistent;
    _id: string;
    __version__: number;
    amorphic: typeof PersistObjectTemplate;
    static __collection__: any;
    static __schema__: any;
    static __table__: any;
    static __parent__: typeof Persistent;

    // New names

    /**
    * 
    * @TODO - ask srksag about why we don't catch for this and why there's no debug log statement here
    * 
    * Delete all objects matching a query
    * @param {JSON} query @TODO
    * @param {JSON} options @TODO
    * @returns {Object}
    */
    static async persistorDeleteByQuery(query, options?) {
        SchemaValidator.validate(options, 'persistSchema', this);
        options = options || {};
        const persistorRef = PersistObjectTemplate;

        if (UtilityFunctions.isDBMongo(persistorRef, this.__collection__)) {
            return await Mongo.deleteByQuery(persistorRef, this, query, options.logger); //@TODO: this doesn't check if logger is set like the others
        }
        else {
            return await PersistObjectTemplate.deleteFromKnexByQuery(this, query, options.transaction, options.logger);
        }
    }

    /**
    * Fetch all objects matching a query
    * @param {JSON} query @TODO
    * @param {JSON} options @TODO
    * @returns {*}
    * 
    * static find<T extends BaseEntity>(this: ObjectType<T>, options?: FindManyOptions<T>): Promise<T[]>;

    */
    static async persistorFetchByQuery (query, options?) {
        SchemaValidator.validate(options, 'fetchSchema', this);

        options = options || {};
        var persistObjectTemplate = options.session || PersistObjectTemplate;
        const usedLogger = options.logger ? options.logger : persistObjectTemplate.logger;
        usedLogger.debug(
            {
                component: 'persistor',
                module: 'api',
                activity: 'persistorFetchByQuery',
                data:
                {
                    template: this.__name__
                }
            });


        // If we are using order, but not sort. Probably to handle an older version of sort
        if (options.order && !options.order.sort) {
            options.order = { sort: options.order };
        }

        try {
            if (UtilityFunctions.isDBMongo(persistObjectTemplate, this.__collection__)) {
                return await Mongo.findByQuery(persistObjectTemplate, this, query, options.fetch, options.start, options.limit, options.transient, options.order, options.order, usedLogger);
            }
            else {
                return await persistObjectTemplate.getFromPersistWithKnexQuery(null, this, query, options.fetch, options.start, options.limit, options.transient, null, options.order, undefined, undefined, usedLogger, options.enableChangeTracking, options.projection);
            }
        } catch (err) {
            // This used to be options.logger || PersistObjectTemplate.logger
            return UtilityFunctions.logExceptionAndRethrow(err, usedLogger, this.__name__, query, 'persistorFetchByQuery');
        }
    }

    /**
    * Return count of objects of this class given a json query
    *
    * @param {json} query mongo style queries
    * @param {object} options @TODO
    * @returns {Number}
    */
    static async persistorCountByQuery(query, options?) {
        SchemaValidator.validate(options, 'fetchSchema', this);

        options = options || {};
        const usedLogger = options.logger ? options.logger : PersistObjectTemplate.logger;
        usedLogger.debug(
            {
                component: 'persistor',
                module: 'api',
                activity: 'getFromPersistWithQuery',
                data:
                {
                    template: this.__name__
                }
            });

        const persistor = PersistObjectTemplate;
        try {
            if (UtilityFunctions.isDBMongo(persistor, this.__collection__)) {
                return await Mongo.countByQuery(persistor, this, query, usedLogger);
            }
            else {
                return await PersistObjectTemplate.countFromKnexQuery(this, query, usedLogger);
            }
        } catch (err) {
            return UtilityFunctions.logExceptionAndRethrow(err, usedLogger, this.__name__, query, { activity: 'persistorCountByQuery' });
        }
    }

    /**
    * Fetch an object by id
    * @param {string} id mongo style id
    * @param {json} options @todo <-- Ask srksag what this TODO is for
    * @returns {*}
    */
    static async persistorFetchById(id, options?) { // @TODO: Legacy <--- Ask srksag what this is for
        SchemaValidator.validate(options, 'fetchSchema', this);

        options = options || {};

        var persistObjectTemplate = options.session || PersistObjectTemplate;
        const usedLogger = options.logger ? options.logger : persistObjectTemplate.logger;

        usedLogger.debug(
            {
                component: 'persistor',
                module: 'api',
                activity: 'persistorFetchById',
                data:
                {
                    template: this.__name__,
                    id: id
                }
            });

        const dbAlias = persistObjectTemplate.getDBAlias(this.__collection__);
        const dbType = persistObjectTemplate.getDB(dbAlias).type;

        try {
            if (dbType == persistObjectTemplate.DB_Mongo) {
                return await persistObjectTemplate.getFromPersistWithMongoId(this, id, options.fetch, options.transient, null, usedLogger); //@TODO: talk this over with srksag changed this from options.logger to usedLogger
            }
            else {
                return await persistObjectTemplate.getFromPersistWithKnexId(this, id, options.fetch, options.transient, null, null, usedLogger, options.enableChangeTracking, options.projection); //@TODO: talk this over with srksag changed this from options.logger to usedLogger
            }
        } catch (err) {
            return UtilityFunctions.logExceptionAndRethrow(err, usedLogger, this.__name__, id, 'persistorFetchById');
        }
    }

    /**
    * Determine whether we are using knex on this table
    * @returns {boolean}
    */
    static persistorIsKnex(): boolean {
        return UtilityFunctions.isDBKnex(PersistObjectTemplate, this.__collection__);
    }

    /**
    * Return knex table name for template for use in native queries
    * @param {string} alias - table alias alias named used when setting the DB object
    * @returns {string}
    */
    static persistorGetTableName(alias?: string): string {
        const tableName = UtilityFunctions.dealias(this.__table__);
        const ifAlias = alias ? ` as ${alias}` : ``;

        return `${tableName}${ifAlias}`;
    }


    /**
    * Return the foreign key for a given parent property for use in native queries
    * @param {string} prop field name
    * @param {string} alias - table alias name used for query generation
    * @returns {string}
    */
    static persistorGetParentKey(prop: string, alias?: string): string {
        const ifAlias = alias ? `${alias}.` : ``;

        return `${ifAlias}${this.__schema__.parents[prop].id}`;
    }

    static persistorGetPrimaryKey(alias?): any { }

    /**
    * Return the foreign key for a given child property for use in native queries
    * @param {string} prop field name
    * @param {string} alias - table alias name used for query generation
    * @returns {string}
    */
    static persistorGetChildKey(prop: string, alias?: string): string {
        const ifAlias = alias ? `${alias}.` : ``;

        return `${ifAlias}${this.__schema__.children[prop].id}`;
    }

    /**
    * Get a knex object that can be used to create native queries (e.g. template.getKnex().select().from())
    * @returns {*}
    */
    static persistorGetKnex() {
        const tableName = UtilityFunctions.dealias(this.__table__);
        const dbType = UtilityFunctions.getDBType(PersistObjectTemplate, this.__table__);

        return dbType.connection(tableName);
    }

    /**
    * return an array of join parameters (e.g. .rightOuterJoin.apply(template.getKnex(), Transaction.knexChildJoin(...)))
    * @param {object} targetTemplate objecttemplate
    * @param {string} primaryAlias - table alias name used for query generation
    * @param {string} targetAlias - table alias name used for query generation
    * @param {string} joinKey - field name
    * @returns {*[]}
    */
    static persistorKnexParentJoin(targetTemplate, primaryAlias: string, targetAlias: string, joinKey: string) {
        return [
            `${this.getTableName()} as ${primaryAlias}`,
            targetTemplate.getParentKey(joinKey, targetAlias),
            this.getPrimaryKey(primaryAlias)
        ];
    }

    /**
    * return an array of join parameters (e.g. .rightOuterJoin.apply(template.getKnex(), Transaction.knexChildJoin(...)))
    * @param {object} targetTemplate target table to join with
    * @param {object} primaryAlias table alias name for the source/current object
    * @param {object} targetAlias table alias name for the target table.
    * @param {string} joinKey source table field name
    * @returns {*[]}
    */
    static persistorKnexChildJoin(targetTemplate, primaryAlias, targetAlias, joinKey: string) {
        return [
            `${this.getTableName()} as ${primaryAlias}`,
            targetTemplate.getChildKey(joinKey, primaryAlias),
            targetTemplate.getPrimaryKey(targetAlias)
        ];
    }

    /**
    * Return '_id'
    * @param {string} alias - table alias name used for query generation
    * @returns {string}
    */
    static persistorGetId(alias?): string {
        const ifAlias = alias ? `${alias}.` : ``;

        return `${ifAlias}_id`;
    }

    async persistorFetchReferences(options) {
        SchemaValidator.validate(options, 'fetchSchema', this.__template__);

        options = options || {};

        const usedLogger = options.logger ? options.logger : PersistObjectTemplate.logger;

        usedLogger.debug(
            {
                component: 'persistor',
                module: 'api',
                activity: 'fetchReferences',
                data:
                {
                    template: this.__template__.__name__,
                    id: this.__id__
                }
            });

        var properties = {}
        var objectProperties = this.__template__.getProperties();
        for (var prop in options.fetch) {
            properties[prop] = objectProperties[prop];
        }

        if (UtilityFunctions.isDBMongo(PersistObjectTemplate, this.__template__.__collection__)) {
            return await Mongo.getTemplateFromPOJO(PersistObjectTemplate, this, this.__template__, null, null, {}, options.fetch, this, properties, options.transient, usedLogger);
        }
        else {
            return await PersistObjectTemplate.getTemplateFromKnexPOJO(this, this.__template__, null, {}, options.fetch, options.transient, null, this, properties, undefined, undefined, undefined, usedLogger)
        }
    }

    async persistorSave(options?) {
        SchemaValidator.validate(options, 'persistSchema', this.__template__);

        options = options || {};
        var txn = PersistObjectTemplate.getCurrentOrDefaultTransaction(options.transaction);
        var cascade = options.cascade;

        const usedLogger = options.logger ? options.logger : PersistObjectTemplate.logger;

        usedLogger.debug(
            {
                component: 'persistor',
                module: 'api',
                activity: 'save',
                data:
                {
                    template: this.__template__.__name__,
                    id: this.__id__
                }
            });

        if (!txn) {
            return await this.persistSave(null, usedLogger);
        }
        else {
            return this.setDirty(txn, false, cascade, usedLogger);
        }
    };
    async persistorRefresh(logger?) {
        const usedLogger = logger ? logger : PersistObjectTemplate.logger;

        usedLogger.debug(
            {
                component: 'persistor',
                module: 'api',
                activity: 'refresh',
                data:
                {
                    template: this.__template__.__name__,
                    id: this.__id__
                }
            });

        //return this.__template__.getFromPersistWithId(this._id, null, null, null, true, logger)
        if (UtilityFunctions.isDBMongo(PersistObjectTemplate, this.__template__.__collection__)) {
            return await Mongo.findById(PersistObjectTemplate, this.__template__, this._id, null, null, null, usedLogger);
        }
        else {
            return await PersistObjectTemplate.getFromPersistWithKnexId(this.__template__, this._id, null, null, null, true, usedLogger);
        }
    }
    
    // persistorDelete will only support new API calls.
    async persistorDelete(options?) { 
        SchemaValidator.validate(options, 'persistSchema', this.__template__);

        options = options || {};
        var txn = UtilityFunctions.getCurrentOrDefaultTransaction(PersistObjectTemplate, options.transaction);
        var cascade = options.cascade;
        const usedLogger = options.logger ? options.logger : PersistObjectTemplate.logger;

        usedLogger.debug(
            {
                component: 'persistor',
                module: 'api',
                activity: 'delete',
                data:
                {
                    template: this.__template__.__name__,
                    id: this.__id__
                }
            });

        if (!txn) {
            return await this.__template__.deleteFromPersistWithId(this._id, null, usedLogger)
        }
        else {
            return PersistObjectTemplate.setAsDeleted(this, txn, false, !cascade, usedLogger);
        }
    }
    async persistorIsStale() {
        const persistObjectTemplate = PersistObjectTemplate;

        let id;
        if (UtilityFunctions.isDBMongo(persistObjectTemplate, this.__template__.__collection__)) {
            id = new ObjectID(this._id.toString())
        }
        else {
            id = this._id;
        }

        const count = await this.__template__.countFromPersistWithQuery(
            {
                _id: id,
                __version__: this.__version__
            });
        return !count;
    }

    // Legacy

    /**
     * Return a single instance of an object of this class given an id
     *
     * @param {string} id mongo style id
     * @param {bool} cascade, loads children if requested
     * @param {bool} isTransient - marking the laoded object as transient.
     * @param {object} idMap id mapper for cached objects
     * @param {bool} isRefresh force load
     * @param {object} logger objecttemplate logger
     * @returns {object}
     */
    static async getFromPersistWithId(id?, cascade?, isTransient?, idMap?, isRefresh?, logger?) {
        const usedLogger = logger ? logger : PersistObjectTemplate.logger;

        usedLogger.debug(
            {
                component: 'persistor',
                module: 'api',
                activity: 'getFromPersistWithId',
                data:
                {
                    template: this.__name__,
                    id: id
                }
            });

        // @TODO: double or triple equals here, ask srksag
        try {
            if (UtilityFunctions.isDBMongo(PersistObjectTemplate, this.__collection__)) {
                return await Mongo.findById(PersistObjectTemplate, this, id, cascade, isTransient, idMap, logger);
            }
            else {
                return await PersistObjectTemplate.getFromPersistWithKnexId(this, id, cascade, isTransient, idMap, isRefresh, logger);
            }
        } catch (err) {
            return UtilityFunctions.logExceptionAndRethrow(err, usedLogger, this.__name__, id, 'getFromPersistWithId')
        }
    }

    /**
    * Return an array of objects of this class given a json query
    *
    * @param {json} query mongo style queries
    * @param {bool} cascade, loads children if requested
    * @param {numeric} start - starting position of the result set.
    * @param {numeric} limit - limit the result set
    * @param {bool} isTransient {@TODO}
    * @param {object} idMap id mapper for cached objects
    * @param {bool} options {@TODO}
    * @param {object} logger objecttemplate logger
    * @returns {object}
    * @deprecated in favor of persistorFetchWithQuery
    */

    static async getFromPersistWithQuery(query, cascade?, start?, limit?, isTransient?, idMap?, options?, logger?) {
        const usedLogger = logger ? logger : PersistObjectTemplate.logger;

        usedLogger.debug(
            {
                component: 'persistor',
                module: 'api',
                activity: 'getFromPersistWithQuery',
                data:
                {
                    template: this.__name__
                }
            });

        try {
            if (UtilityFunctions.isDBMongo(PersistObjectTemplate, this.__collection__)) {
                return await Mongo.findByQuery(PersistObjectTemplate, this, query, cascade, start, limit, isTransient, idMap, options, logger);
            }
            else {
                return await PersistObjectTemplate.getFromPersistWithKnexQuery(null, this, query, cascade, start, limit, isTransient, idMap, options, undefined, undefined, logger);
            }
        } catch (err) {
            return UtilityFunctions.logExceptionAndRethrow(err, usedLogger, this.__name__, query, 'getFromPersistWithQuery');
        }
    }


    /**
     * Delete objects given a json query
     *
     * @param {json} query mongo style queries
     * @param {object} txn persistObjectTemplate transaciton object
     * @param {object} logger objecttemplate logger
     * @returns {object}
     * @deprecated in favor of persistorDeleteByQuery
     * 
     * @TODO: No error handling here or logging here, talk to srksag
     */
    static async deleteFromPersistWithQuery(query, txn?, logger?) {
        if (UtilityFunctions.isDBMongo(PersistObjectTemplate, this.__collection__)) {
            return await Mongo.deleteByQuery(PersistObjectTemplate, this, query, logger);
        }
        else {
            return await PersistObjectTemplate.deleteFromKnexQuery(this, query, txn, logger);
        }
    }

    /**
     * Delete objects given an id
     *
     * @param {string} id mongo style id
     * @param {object} txn persistObjectTemplate transaciton object
     * @param {object} logger objecttemplate logger
     * @returns {object}
     * @deprecated in favor of persistorDeleteById - THIS DOESN"T EXIST. We need to UNDEPRECATE
     */
    static async deleteFromPersistWithId(id, txn?, logger?) {

        const usedLogger = logger ? logger : PersistObjectTemplate.logger;
        usedLogger.debug(
            {
                component: 'persistor',
                module: 'api',
                activity: 'deleteFromPersistWithId',
                data:
                {
                    template: this.__name__
                }
            });

        try {
            if (UtilityFunctions.isDBMongo(PersistObjectTemplate, this.__collection__)) {
                return await Mongo.deleteById(PersistObjectTemplate, this, id, usedLogger);
            }
            else {
                return await PersistObjectTemplate.deleteFromKnexId(this, id, txn, usedLogger);
            }
        } catch (err) {
            return UtilityFunctions.logExceptionAndRethrow(err, usedLogger, this.__name__, id, { activity: 'deleteFromPersistWithId' });
        }
    }

    /**
    * Return count of objects of this class given a json query
    *
    * @param {json} query mongo style queries
    * @param {object} logger objecttemplate logger
    * @returns {Number}
    * @deprecated in favor of persistorCountWithQuery
    */
    static async countFromPersistWithQuery(query?, logger?) {
        const usedLogger = logger ? logger : PersistObjectTemplate.logger;
        usedLogger.debug(
            {
                component: 'persistor',
                module: 'api',
                activity: 'countFromPersistWithQuery',
                data:
                {
                    template: this.__name__
                }
            });


        try {
            if (UtilityFunctions.isDBMongo(PersistObjectTemplate, this.__collection__)) {
                return await Mongo.countByQuery(PersistObjectTemplate, this, query, usedLogger);
            }
            else {
                return await PersistObjectTemplate.countFromKnexQuery(this, query, usedLogger);
            }
        } catch (err) {
            return UtilityFunctions.logExceptionAndRethrow(err, usedLogger, this.__name__, query, 'countFromPersistWithQuery');
        }
    }

    // Legacy
    async fetchProperty(prop, cascade?, queryOptions?, isTransient?, idMap?, logger?) {
        const usedLogger = logger ? logger : PersistObjectTemplate.logger;
        usedLogger.debug(
            {
                component: 'persistor',
                module: 'api',
                activity: 'fetchProperty',
                data:
                {
                    template: this.__template__.__name__,
                    id: this.__id__
                }
            });


        idMap = idMap || {};
        var properties = {};
        var objectProperties = this.__template__.getProperties();
        properties[prop] = objectProperties[prop];

        if (queryOptions) {
            properties[prop].queryOptions = queryOptions;
        }
        var cascadeTop = {};
        cascadeTop[prop] = cascade || true;

        if (UtilityFunctions.isDBMongo(PersistObjectTemplate, this.__template__.__collection__)) {
            return await Mongo.getTemplateFromPOJO(PersistObjectTemplate, this, this.__template__, null, null, idMap, cascadeTop, this, properties, isTransient, usedLogger);
        }
        else {
            return await PersistObjectTemplate.getTemplateFromKnexPOJO(this, this.__template__, null, idMap, cascadeTop, isTransient, null, this, properties, undefined, undefined, undefined, usedLogger);
        }
    }

    async fetch(cascade, isTransient?, idMap?, logger?) {
        const usedLogger = logger ? logger : PersistObjectTemplate.logger;
        usedLogger.debug(
            {
                component: 'persistor',
                module: 'api',
                activity: 'fetch',
                data:
                {
                    template: this.__template__.__name__,
                    id: this.__id__
                }
            });
        idMap = idMap || {};

        var properties = {}
        var objectProperties = this.__template__.getProperties();
        for (var prop in cascade) {
            properties[prop] = objectProperties[prop];
        }


        var previousDirtyTracking = PersistObjectTemplate.__changeTracking__;
        PersistObjectTemplate.__changeTracking__ = false;

        try {
            if (UtilityFunctions.isDBMongo(PersistObjectTemplate, this.__template__.__collection__)) {
                return await Mongo.getTemplateFromPOJO(PersistObjectTemplate, this, this.__template__, null, null, idMap, cascade, this, properties, isTransient, usedLogger);
            }
            else {
                return await PersistObjectTemplate.getTemplateFromKnexPOJO(this, this.__template__, null, idMap, cascade, isTransient, null, this, properties, undefined, undefined, undefined, usedLogger);
            }
        }
        finally {
            PersistObjectTemplate.__changeTracking__ = previousDirtyTracking;
        }
    }

    // Legacy 
    async fetchReferences(options) {
        return await this.persistorFetchReferences(options);
    }

    /**
     * @legacy 
     *
     * @param {*} [txn]
     * @param {*} [logger]
     * @returns {*}
     * @memberof Persistent
     */

    // @TODO: ask srksag if this is async
    async persistSave(txn?, logger?) {
        // var persistObjectTemplate = this.__objectTemplate__ || self; //@TODO: ask srksag is this ever set (this.__objectTemplate__)

        const persistObjectTemplate = PersistObjectTemplate;
        const usedLogger = logger ? logger : PersistObjectTemplate.logger;

        usedLogger.debug(
            {
                component: 'persistor',
                module: 'api',
                activity: 'persistSave',
                data:
                {
                    template: this.__template__.__name__,
                    id: this.__id__
                }
            });

        //@TODO: Ask srksag how come there's no catch for errors here

        if (UtilityFunctions.isDBMongo(PersistObjectTemplate, this.__template__.__collection__)) {
            const returnVal = await Mongo.persistSave(PersistObjectTemplate, this, undefined, undefined, undefined, txn, logger)
            if (txn) {
                UtilityFunctions.saved(PersistObjectTemplate, returnVal, txn); //@TODO: might need to await here
            }
            return await returnVal._id.toString(); //@TODO: do we need to await here? we already awaited returnval
        }
        else {
            const returnVal = await persistObjectTemplate.persistSaveKnex(this, txn, logger);
            if (txn) {
                UtilityFunctions.saved(PersistObjectTemplate, returnVal, txn);
            }

            return await returnVal._id.toString(); //@TODO: do we need to await here? we already awaited returnval
        }
    }

    // Legacy -- just use persistorSave
    async persistTouch(txn?, logger?) {
        const persistObjectTemplate = PersistObjectTemplate;
        const usedLogger = logger ? logger : PersistObjectTemplate.logger;

        usedLogger.debug(
            {
                component: 'persistor',
                module: 'api',
                activity: 'persistTouch',
                data:
                {
                    template: this.__template__.__name__,
                    id: this.__id__
                }
            });

        //@TODO: Ask srksag how come there's no catch for errors here

        if (UtilityFunctions.isDBMongo(PersistObjectTemplate, this.__template__.__collection__)) {
            return await Mongo.persistSave(PersistObjectTemplate, this, undefined, undefined, undefined, txn, logger);
        }
        else {
            return await persistObjectTemplate.persistTouchKnex(this, txn, logger);
        }
    }

    //persistDelete is modified to support both legacy and V2, options this is passed for V2 as the first parameter.

    // Legacy
    async persistDelete(txn?, logger?) {

        if (!txn || (txn && txn.knex && txn.knex.transacting)) {

            const usedLogger = logger ? logger : PersistObjectTemplate.logger;

            usedLogger.debug(
                {
                    component: 'persistor',
                    module: 'api',
                    activity: 'persistDelete',
                    data:
                    {
                        template: this.__template__.__name__,
                        id: this.__id__
                    }
                });

            if (txn) {
                delete txn.dirtyObjects[this.__id__];
            }
            return await this.__template__.deleteFromPersistWithId(this._id, txn, logger)
        }
        else {
            //for V2 options are passed as the first parameter -- @TODO ask srksag about this
            return await this.deleteV2.call(this, txn);
        }
    }

    // Legacy
    cascadeSave(txn?, logger?) {
        const transaction = txn || PersistObjectTemplate.currentTransaction;
        return PersistObjectTemplate.setDirty(this, transaction, true, false, logger);
    }

    // Legacy
    async isStale() {
        return await this.persistorIsStale();
    }

    // Legacy
    async persist(options) {
        return await this.persistorSave(options);
    }

    // Legacy --- @TODO: ask srksag, why this is called noCascade,  but setDirty is asking for !cascade

    //Original: setDirty(txn?, onlyIfChanged?, noCascade?, logger?)  {
    setDirty(txn?, onlyIfChanged?, noCascade?, logger?) {
        //Original: PersistObjectTemplate.setDirty(this, txn, onlyIfChanged, !cascade, logger);
        return PersistObjectTemplate.setDirty(this, txn, onlyIfChanged, !noCascade, logger);
    }


    setAsDeleted(txn?, onlyIfChanged?) {
        return PersistObjectTemplate.setAsDeleted(this, txn, onlyIfChanged)
    }

    // Legacy
    async refresh(logger?) {
        return await this.persistorRefresh(logger);
    };

    /**
    * Inject the persitor properties and get/fetch methods need for persistence.  This is either called
    * as part of _injectTemplate if the template was fully created or when the template is instantiated lazily
    * May not be needed in Typescript path anymore
    * @static
    * @protected
    * @returns
    * @memberof Persistent
    */
   protected static _injectProperties() {
        if (this.hasOwnProperty('__propertiesInjected__'))
            return;
        const props = this.defineProperties;
        for (var prop in props) {
            const defineProperty = props[prop];
            const type = defineProperty.type;
            const of = defineProperty.of;
            const refType = of || type;

            let template = this;
            if (refType && refType.isObjectTemplate && UtilityFunctions._persistProperty(PersistObjectTemplate, defineProperty)) {
                var isCrossDocRef = PersistObjectTemplate.isCrossDocRef(template, prop, defineProperty)
                if (isCrossDocRef || defineProperty.autoFetch) {
                    (function () {
                        var closureProp = prop;
                        var closureFetch = defineProperty.fetch ? defineProperty.fetch : {};
                        var closureQueryOptions = defineProperty.queryOptions ? defineProperty.queryOptions : {};
                        var toClient = !(defineProperty.isLocal || (defineProperty.toClient === false))
                        if (!props[closureProp + 'Persistor']) {
                            template.createProperty(closureProp + 'Persistor', {
                                type: Object, toClient: toClient,
                                toServer: false, persist: false,
                                value: { isFetched: defineProperty.autoFetch ? false : true, isFetching: false }
                            });
                        }
                        if (!template.prototype[closureProp + 'Fetch'])
                            template.createProperty(closureProp + 'Fetch', {
                                on: 'server', body: function (start, limit) {
                                    if (typeof (start) != 'undefined') {
                                        closureQueryOptions['skip'] = start;
                                    }
                                    if (typeof (limit) != 'undefined') {
                                        closureQueryOptions['limit'] = limit;
                                    }
                                    return this.fetchProperty(closureProp, closureFetch, closureQueryOptions);
                                }
                            });
                    })();
                }
            }
        }
        this.__propertiesInjected__ = true;
    }

    // Deprecated API (DO NOT USE THESE)
    static isKnex() {
        return this.persistorIsKnex();
    }

    static getKnex() {
        return this.persistorGetKnex();
    }

    static getTableName(alias?: string) {
        return this.persistorGetTableName(alias);
    }

    static getParentKey(prop: string, alias?: string) {
        return this.persistorGetParentKey(prop, alias);
    }

    static getChildKey(prop: string, alias?: string) {
        return this.persistorGetChildKey(prop, alias);
    }

    static getPrimaryKey(alias?: string) {
        return this.persistorGetId(alias);
    }

    static knexParentJoin(targetTemplate, primaryAlias: string, targetAlias: string, joinKey: string) {
        return this.persistorKnexParentJoin(targetTemplate, primaryAlias, targetAlias, joinKey);
    }

    static knexChildJoin(targetTemplate, primaryAlias, targetAlias, joinKey: string) {
        return this.persistorKnexChildJoin(targetTemplate, primaryAlias, targetAlias, joinKey);
    }

    async deleteV2(options) {
        return await this.persistorDelete();
    }
};