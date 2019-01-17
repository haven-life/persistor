import * as _ from 'underscore';
import { PersistObjectTemplate } from './PersistObjectTemplate';
import { Persistent } from './Persistent';
import { ObjectID } from 'mongodb';
export namespace UtilityFunctions {
    export function processTemplate(template, persistorProps) {

        let props = template.getProperties();

        _.each(props, (_defineProperty, prop: string) => {
            if (prop.match(/Persistor$/) && prop.substr(0, 2) != '__') {
                persistorProps[template.__name__] = persistorProps[template.__name__] || {}
                persistorProps[template.__name__][prop.replace(/Persistor$/, '')] = 1;
            }
        });
    }

    function undefHandler(key, value) {
        return typeof (value) === 'undefined' ? `undefined value provided for ${key}` : value;
    }

    export function logExceptionAndRethrow(exception, logger, template, query, activity) {
        if (typeof (query) === 'undefined') {
            query = 'Undefined value provided for query';
        } else if (typeof (query) === 'object') {
            query = JSON.stringify(query, undefHandler);
        }

        logger.error(
            {
                component: 'persistor',
                module: 'api',
                activity: activity,
                data:
                {
                    template: template,
                    query: query
                }
            });

        throw exception;
    }

    // PersistObjectTemplate.createTransientObject
    export function createTransientObject(persistor: typeof PersistObjectTemplate, callback) {
        const currentState = persistor.__transient__;
        persistor.__transient__ = true;

        let obj = null;
        if (typeof (callback) === 'function') {
            obj = callback();
        }

        persistor.__transient__ = currentState || false;
        return obj;
    }

    // PersistObjectTemplate.saved
    export function saved(persistor: typeof PersistObjectTemplate, obj, txn) {
        delete obj['__dirty__'];
        delete obj['__changed__'];

        var savedObjects = txn ? txn.savedObjects : persistor.savedObjects;

        if (savedObjects) {
            savedObjects[obj.__id__] = obj;
        }
    }

    /**
     * Walk one-to-one links to arrive at the top level document
     * @param {Supertype} obj - subdocument object to start at
     * @returns {Supertype}
     * 
     * PersistObjectTemplate.getTopObject 
     */
    export function getTopObject(persistor: typeof PersistObjectTemplate, obj) {
        const idMap = {};
        const traverse = (obj) => {
            idMap[obj.__id__] = obj;
            if (obj.__template__.__schema__.documentOf) {
                return obj;
            }

            const props = obj.__template__.getProperties();
            for (const prop in props) {
                const type = props[prop].type;
                const value = obj[prop];

                if (type && value && value.__id__ && !idMap[value.__id__]) {
                    const traversedObj = traverse(value);
                    if (traversedObj)
                        return traversedObj;
                }
            }

            return false;
        }

        return traverse(obj);
    }

    /**
     * Walk through all objects in a document from the top
     * @param {Supertype} obj - subdocument object to start at
     * @param {function} callback - to add any custom behavior
     * @returns {Supertype}
     * 
     * PersistObjectTemplate.enumerateDocumentObjects 
     */
    export function enumerateDocumentObjects(persistor: typeof PersistObjectTemplate, obj, callback) {

        const idMap = {};
        const traverse = (obj, ...args) => {
            if (obj) {
                callback(obj);

                const props = obj.__template__.getProperties();
                _.map(props, (defineProperty, prop) => {

                    const idMapEntry = `${obj.__id__}_${prop}`;

                    if (defineProperty.type == Array && defineProperty.of && defineProperty.of.isObjectTemplate) {

                        if (!idMap[idMapEntry]) {

                            idMap[idMapEntry] = true;
                            _.map(obj[prop], (value) => {
                                traverse(value, obj, prop);
                            });
                        }
                    }

                    if (defineProperty.type && defineProperty.type.isObjectTemplate) {
                        if (obj[prop]) {
                            if (!idMap[idMapEntry]) {
                                idMap[idMapEntry] = true;
                                traverse(obj[prop], obj, prop);
                            }
                        }
                    }
                });
            }
        }

        return traverse(obj);
    }

    // persistObjectTemplate.getTemplateByCollection
    export function getTemplateByCollection(persistor: typeof PersistObjectTemplate, collection) {
        Object.keys(persistor._schema).forEach((prop) => {
            if (persistor._schema[prop].documentOf == collection) {
                return persistor.getTemplateByName(prop);
            }
        });

        throw new Error(`Cannot find template for ${collection}`);
    }

    //  pOT.checkObject
    export function checkObject(obj: Persistent) {
        if (!obj.__template__) {
            throw new Error('Attempted to save a non-templated Object');
        }

        if (!obj.__template__) {
            throw new Error(`Schema entry missing for ${obj.__template__.__name__}`);
        }
    }

    export function createPrimaryKey(persistor: typeof PersistObjectTemplate, obj) {
        const objectId = new ObjectID();
        const key = objectId.toString();

        if (persistor.objectMap && !obj.__transient__) {
            persistor.objectMap[key] = obj.__id__;
        }

        return key;
    }

    export function getObjectId(persistor: typeof PersistObjectTemplate, _template, pojo, prefix) {

        let index = pojo[`${prefix}_id`].toString();

        if (persistor.objectMap && persistor.objectMap[index]) {
            return persistor.objectMap[index];
        }
        else {
            return `persist-${_template.name}-${index}`;
        }
    }

    export function _persistProperty(persistor: typeof PersistObjectTemplate, defineProperty) {
        if (defineProperty.persist == false || defineProperty.isLocal == true) {
            return false;
        }
        else {
            return true;
        }
    }

    export function getDB(persistor: typeof PersistObjectTemplate, alias) {

        if (!persistor._db) {
            throw new Error('You must do PersistObjectTempate.setDB()');
        }

        let index = alias || '__default__';

        if (!this._db[index]) {
            throw new Error(`DB Alias ${index} not set with corresponding UtilityFunctions.setDB(db, type, alias)`);
        }

        return this._db[index];
    }

    export function dealias(collection) {
        return collection.replace(/\:.*/, '').replace(/.*\//, '');
    }

    export function getDBAlias(collection: string): string {
        if (!collection) {
            return '__default__';
        }

        return collection.match(/(.*)\//) ? RegExp.$1 : '__default__'
    }

    export function getDBID (masterId) {
        if (!masterId) {
            return new ObjectID();
        }
        else {
            return `${masterId.toString()}:${new ObjectID().toString()}`;
        }
    }

    export async function resolveRecursivePromises (promises, returnValue) {

        await Promise.all(promises);

        if (promises.length > 0 ) {
            return resolveRecursivePromises(promises, returnValue);
        }
        else {
            return returnValue;
        }
    }

    export function getCurrentOrDefaultTransaction (persistor: typeof PersistObjectTemplate, current) {

        if (!!current) {
            return persistor.__defaultTransaction__;
        }
        else {
            return current;
        }
    }

}
