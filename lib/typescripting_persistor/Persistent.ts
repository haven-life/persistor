// Should default to Supertype's Constructor

import { Supertype } from 'supertype';
import { PersistObjectTemplate } from './PersistObjectTemplate';
import { UtilityFunctions } from './UtilityFunctions';


export class Persistent extends Supertype {

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
        PersistObjectTemplate._validateParams(options, 'persistSchema', this);
        options = options || {};

        const dbAlias = PersistObjectTemplate.getDBAlias(this.__collection__);
        const dbType = PersistObjectTemplate.getDB(dbAlias).type;

        if (dbType == PersistObjectTemplate.DB_Mongo) {
            return await PersistObjectTemplate.deleteFromPersistWithMongoQuery(this, query, options.logger); //@TODO: this doesn't check if logger is set like the others
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
    */
    static async persistorFetchByQuery(query, options?) {
        PersistObjectTemplate._validateParams(options, 'fetchSchema', this);

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

        const dbAlias = persistObjectTemplate.getDBAlias(this.__collection__);
        const dbType = persistObjectTemplate.getDB(dbAlias).type;

        // If we are using order, but not sort. Probably to handle an older version of sort
        if (options.order && !options.order.sort) {
            options.order = { sort: options.order };
        }

        try {
            if (dbType == persistObjectTemplate.DB_Mongo) {
                return await persistObjectTemplate.getFromPersistWithMongoQuery(this, query, options.fetch, options.start, options.limit, options.transient, options.order, options.order, usedLogger);
            }
            else {
                return await persistObjectTemplate.getFromPersistWithKnexQuery(null, this, query, options.fetch, options.start, options.limit, options.transient, null, options.order, undefined, undefined, usedLogger, options.enableChangeTracking, options.projection));
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
        PersistObjectTemplate._validateParams(options, 'fetchSchema', this);

        options = options || {};
        const usedLogger = options.logger ? options.logger : PersistObjectTemplate.logger;
        usedLogger.debug(
            {
                component: 'persistor',
                module: 'api',
                activity: 'getFromPersistWithQuery',
                data:
                {
                    template: template.__name__
                }
            });


        const dbAlias = PersistObjectTemplate.getDBAlias(this.__collection__);
        const dbType = PersistObjectTemplate.getDB(dbAlias).type;


        try {
            if (dbType == PersistObjectTemplate.DB_Mongo) {
                return await PersistObjectTemplate.countFromMongoQuery(this, query, usedLogger);
            }
            else {
                return await PersistObjectTemplate.countFromKnexQuery(this, query, usedLogger))
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
        PersistObjectTemplate._validateParams(options, 'fetchSchema', this);

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
        const dbAlias = PersistObjectTemplate.getDBAlias(this.__collection__);
        const dbType = PersistObjectTemplate.getDB(dbAlias).type;

        return dbType != PersistObjectTemplate.DB_Mongo;
    }

    /**
    * Return knex table name for template for use in native queries
    * @param {string} alias - table alias alias named used when setting the DB object
    * @returns {string}
    */
    static persistorGetTableName(alias?: string): string {
        const tableName = PersistObjectTemplate.dealias(this.__table__);
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
        const tableName = PersistObjectTemplate.dealias(this.__table__);
        const dbAlias = PersistObjectTemplate.getDBAlias(this.__table__);
        const dbType = PersistObjectTemplate.getDB(dbAlias).type;

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

    persistorSave(options?): any { };
    persistorRefresh(logger?): any { }
    persistorDelete(options?): any { };
    persistorIsStale(): any { }

    _id: string;
    __version__: number;
    amorphic: Persistor;

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

        const dbAlias = PersistObjectTemplate.getDBAlias(this.__collection__);
        const dbType = PersistObjectTemplate.getDB(dbAlias).type;

        // @TODO: double or triple equals here, ask srksag
        try {
            if (dbType == PersistObjectTemplate.DB_Mongo) {
                return await PersistObjectTemplate.getFromPersistWithMongoId(this, id, cascade, isTransient, idMap, logger);
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

        const dbAlias = PersistObjectTemplate.getDBAlias(this.__collection__);
        const dbType = PersistObjectTemplate.getDB(dbAlias).type;

        try {
            if (dbType == PersistObjectTemplate.DB_Mongo) {
                return await PersistObjectTemplate.getFromPersistWithMongoQuery(this, query, cascade, start, limit, isTransient, idMap, options, logger);
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
        const dbAlias = PersistObjectTemplate.getDBAlias(this.__collection__);
        const dbType = PersistObjectTemplate.getDB(dbAlias).type;

        if (dbType == PersistObjectTemplate.DB_Mongo) {
            return await PersistObjectTemplate.deleteFromPersistWithMongoQuery(this, query, logger);
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
     * @deprecated in favor of persistorDeleteById
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

        const dbAlias = PersistObjectTemplate.getDBAlias(this.__collection__);
        const dbType = PersistObjectTemplate.getDB(dbAlias).type;

        try {
            if (dbType == PersistObjectTemplate.DB_Mongo) {
                return await PersistObjectTemplate.deleteFromPersistWithMongoId(template, id, usedLogger);
            }
            else {
                return await PersistObjectTemplate.deleteFromKnexId(template, id, txn, usedLogger);
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
    static countFromPersistWithQuery(query?, logger?) {
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

        const dbAlias = PersistObjectTemplate.getDBAlias(this.__collection__);
        const dbType = PersistObjectTemplate.getDB(dbAlias).type;

        try {
            if (dbType == PersistObjectTemplate.DB_Mongo) {
                return await PersistObjectTemplate.countFromMongoQuery(this, query, usedLogger);
            }
            else {
                return await PersistObjectTemplate.countFromKnexQuery(this, query, usedLogger);
            }
        } catch (err) {
            return UtilityFunctions.logExceptionAndRethrow(err, usedLogger, this.__name__, query, 'countFromPersistWithQuery');
        }
    }


    fetchProperty(prop, cascade?, queryOptions?, isTransient?, idMap?, logger?): any { }
    fetch(cascade, isTransient?, idMap?, logger?): any { }
    fetchReferences(options): any { }
    persistSave(txn?, logger?): any { }
    persistTouch(txn?, logger?): any { }
    persistDelete(txn?, logger?): any { }
    cascadeSave(any): any { }
    isStale(): any { }
    persist(options): any { }
    setDirty(txn?, onlyIfChanged?, noCascade?, logger?): any { }
    setAsDeleted(txn?, onlyIfChanged?): any { }
    refresh(logger?): any { };

    getTableName(): any { }
    getParentKey(): any { }


    /**
    * Inject the persitor properties and get/fetch methods need for persistence.  This is either called
    * as part of _injectTemplate if the template was fully created or when the template is instantiated lazily
    * May not be needed in Typescript path anymore
    * @static
    * @protected
    * @returns
    * @memberof Persistent
    */
    static protected _injectProperties() {
        if (this.hasOwnProperty('__propertiesInjected__'))
            return;
        const props = this.defineProperties;
        for (var prop in props) {
            const defineProperty = props[prop];
            const type = defineProperty.type;
            const of = defineProperty.of;
            const refType = of || type;

            let template = this;
            if (refType && refType.isObjectTemplate && PersistObjectTemplate._persistProperty(defineProperty)) {
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
};