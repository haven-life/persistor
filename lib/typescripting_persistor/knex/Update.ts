import * as _ from 'underscore';
import { PersistObjectTemplate } from '../PersistObjectTemplate';
import { UtilityFunctions } from '../UtilityFunctions';
import { Database } from './Database';

export namespace Update {

    /**
     * Save the object to persistent storage
     *
     * A copy of the object is made which has only the persistent properties
     * and all objects references for objects not stored in the the document
     * replaced by foreign keys.  Arrays of objects not stored in the document
     * are adjusted such that their foreign keys point back to this object.
     * Any related objects stored in other documents are also saved.
     *
     * @param {object} obj  Only required parameter - the object to be saved
     * @param {object} txn transaction object -- can be used only in the end trasaction callback.
     * @param {object} logger object template logger
     * @returns {*}
     */
    export async function persistSaveKnex(persistor: typeof PersistObjectTemplate, obj, txn, logger) {
        const usedLogger = logger || persistor.logger;
        usedLogger.debug({
            component: 'persistor',
            module: 'db.persistSaveKnex',
            activity: 'pre',
            data: {
                template: obj.__template__.__name__,
                id: obj.__id__,
                _id: obj._id
            }
        });
        UtilityFunctions.checkObject(obj);

        const template = obj.__template__;
        const schema = template.__schema__;
        const templateName = template.__name__;
        const isDocumentUpdate = obj.__version__ ? true : false;
        const props = template.getProperties();
        const promises = [];
        const dataSaved = {};

        obj._id = obj._id || UtilityFunctions.createPrimaryKey(persistor, obj);
        var pojo = { _template: obj.__template__.__name__, _id: obj._id };


        /**
         *  Walk through all the properties and copy them to POJO with special treatment for
         *  references to templated objects where we have to maintain foreign key relationships
         */

        for (const prop in props) {

            const defineProperty = props[prop];
            const value = obj[prop];
            const objProp = `${prop}Persistor`;

            // Deal with properties we don't plan to save
            if (!UtilityFunctions._persistProperty(defineProperty) || !defineProperty.enumerable || typeof (value) == 'undefined' || value == null) {

                // Make sure we don't wipe out foreign keys of non-cascaded object references
                if (checkDefineProperties(defineProperty) && checkObjProperties(obj, objProp) && checkSchemaExists(schema, prop)) {
                    pojo[schema.parents[prop].id] = obj[objProp].id;
                    continue;
                }

                if (value != null) {
                    continue;
                }
            }

            // Handle Arrays
            if (defineProperty.type == Array && defineProperty.of.isObjectTemplate) {
                // Arrays of Pojos just get saved
                if (!defineProperty.of.__table__) {
                    pojo[prop] = value;

                    // Templated arrays we need to make sure their foreign keys are up-to-date
                }
                else if (value instanceof Array) {

                    if (!schema.children[prop]) {
                        throw new Error(`Missing children entry for ${prop} in ${templateName}`);
                    }

                    var childForeignKey = schema.children[prop].id;
                    if (schema.children[prop].filter && (!schema.children[prop].filter.value || !schema.children[prop].filter.property)) {
                        throw new Error(`Incorrect filter properties on ${prop} in ${templateName}`);
                    }

                    var foreignFilterKey = schema.children[prop].filter ? schema.children[prop].filter.property : null;
                    var foreignFilterValue = schema.children[prop].filter ? schema.children[prop].filter.value : null;

                    value.forEach((referencedObj, ix) => {
                        if (!referencedObj) {
                            usedLogger.debug(
                                {
                                    component: 'persistor', module: 'db.persistSaveKnex'
                                }, `${obj.__id__}.${prop}[${ix}] is null`);
                            return;
                        }

                        if (!defineProperty.of.__schema__.parents) {
                            throw new Error(`Missing parent entry in ${defineProperty.of.__name__} for ${templateName}`);
                        }

                        // Go through each of the parents in the schema to find the one matching this reference
                        _.each(defineProperty.of.__schema__.parents, (parentSchemaEntry: any, parentProp) => {
                            const parentPersistor = `${parentProp}Persistor`;

                            if (parentSchemaEntry.id == childForeignKey) {

                                // If anything is missing in the child such as the persistor property not having been
                                // setup or the filter property not being setup, fill in and set it dirty
                                if (!referencedObj[parentPersistor] || !referencedObj[parentPersistor].id || referencedObj[parentPersistor].id != obj._id || (foreignFilterKey ? referencedObj[foreignFilterKey] != foreignFilterValue : false)) {

                                    // Take care of filter property
                                    if (foreignFilterKey) {
                                        referencedObj[foreignFilterKey] = foreignFilterValue;
                                    }

                                    // Force parent pointer
                                    if (referencedObj[parentProp] != obj) {
                                        referencedObj[parentProp] = obj;
                                    }

                                    referencedObj.setDirty(txn);
                                }
                            }
                        });

                        if (!referencedObj._id) {
                            referencedObj._id = UtilityFunctions.createPrimaryKey(persistor, referencedObj);
                        }
                    });
                    
                    if (schema.children[prop].pruneOrphans && obj[objProp].isFetched) {
                        promises.push(Database.knexPruneOrphans(persistor, obj, prop, txn, foreignFilterKey, foreignFilterValue, logger));
                    }
                }

                updatePersistorProp(obj, objProp, { isFetching: false, isFetched: true });

                // One-to-One
            }
            else if (defineProperty.type && defineProperty.type.isObjectTemplate) {
                // Make sure schema is in order
                if (!schema || !schema.parents || !schema.parents[prop] || !schema.parents[prop].id) {
                    throw new Error(`${obj.__template__.__name__}.${prop} is missing a parents schema entry`);
                }

                const foreignKey = (schema.parents && schema.parents[prop]) ? schema.parents[prop].id : prop;
                if (value && !value._id) {
                    value._id = UtilityFunctions.createPrimaryKey(persistor, value);
                    value.setDirty(txn);
                }

                pojo[foreignKey] = value ? value._id : null
                updatePersistorProp(obj, objProp, { isFetching: false, id: value ? value._id : null, isFetched: true })

                dataSaved[foreignKey] = pojo[foreignKey] || 'null';
            }
            else if (defineProperty.type == Array || defineProperty.type == Object) {
                pojo[prop] = (obj[prop] === null || obj[prop] === undefined) ? null : JSON.stringify(obj[prop]);
                logChanges(defineProperty, pojo, prop, dataSaved);
            }
            else if (defineProperty.type == Date) {
                pojo[prop] = obj[prop] ? obj[prop] : null;
                logChanges(defineProperty, pojo, prop, dataSaved);
            }
            else if (defineProperty.type == Boolean) {
                pojo[prop] = obj[prop] == null ? null : (obj[prop] ? true : false);
                logChanges(defineProperty, pojo, prop, dataSaved);
            }
            else {
                pojo[prop] = obj[prop];
                logChanges(defineProperty, pojo, prop, dataSaved);
            }
        }

        usedLogger.debug({
            component: 'persistor',
            module: 'db',
            activity: 'dataLogging',
            data: {
                template: obj.__template__.__name__,
                _id: pojo._id,
                values: dataSaved
            }
        });
        
        promises.push(Database.saveKnexPojo(persistor, obj, pojo, isDocumentUpdate ? obj._id : null, txn, logger));

        await Promise.all(promises);
        return obj;
    }

    function checkDefineProperties(defineProperty) {
        return defineProperty.type != Array && defineProperty.type && defineProperty.type.isObjectTemplate;
    }

    function checkObjProperties(obj, objProp) {
        return obj[objProp] && !obj[objProp].isFetched && obj[objProp].id;
    }

    function checkSchemaExists(schema, prop) {
        return schema && schema.parents && schema.parents[prop] && schema.parents[prop].id;
    }

    // was just log
    function logChanges(defineProperty, pojo, prop, dataSaved) {
        if (defineProperty.logChanges) {
            dataSaved[prop] = pojo[prop];
        }
    }

    function copyProps(obj) {
        const newObj = {};
        for (const prop in obj) {
            newObj[prop] = obj[prop];
        }

        return newObj;
    }

    function updatePersistorProp(obj, prop, values) {
        if (!obj[prop])
            obj[prop] = {};
        var modified = false;

        _.map(values, (value, key) => {
            if (obj[prop][key] != value) {
                obj[prop][key] = value;
                modified = true;
            }
        });

        if (modified) {
            obj[prop] = copyProps(obj[prop]);
        }
    }
}