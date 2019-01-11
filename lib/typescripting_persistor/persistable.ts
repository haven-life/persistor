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

    static persistorFetchByQuery(query, options?): any { }
    static persistorCountByQuery(query, options?): any { }

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
                return await persistObjectTemplate.getFromPersistWithMongoId(this, id, options.fetch, options.transient, null, options.logger);
            }
            else {
                return await persistObjectTemplate.getFromPersistWithKnexId(this, id, options.fetch, options.transient, null, null, options.logger, options.enableChangeTracking, options.projection);
            }
        } catch (err) {
            return UtilityFunctions.logExceptionAndRethrow(err, usedLogger, this.__name__, id, 'persistorFetchById');
        }
    }

    static persistorIsKnex(): any { }
    static persistorGetTableName(alias?): any { }
    static persistorGetParentKey(prop, alias?): any { }
    static persistorGetPrimaryKey(alias?): any { }
    static persistorGetChildKey(prop, alias?): any { }
    static persistorGetKnex(): any { }
    static persistorKnexParentJoin(targetTemplate, primaryAlias, targetAlias, joinKey): any { }
    static persistorKnexChildJoin(targetTemplate, primaryAlias, targetAlias, joinKey): any { }

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

    static deleteFromPersistWithId(id, txn?, logger?): any { }
    static countFromPersistWithQuery(query?, logger?): any { }
    static getTableName(alias?): any { }
    static getParentKey(prop, alias?): any { }
    static getPrimaryKey(alias?): any { }
    static getChildKey(prop, alias?): any { }
    static getKnex(): any { }
    static isKnex(): any { }
    static knexParentJoin(targetTemplate, primaryAlias, targetAlias, joinKey): any { }
    static knexChildJoin(targetTemplate, primaryAlias, targetAlias, joinKey): any { }

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
};

export function Persistable<BC extends SupertypeConstructor>(Base: BC) {

    return class Persistable extends Base {

    }
}