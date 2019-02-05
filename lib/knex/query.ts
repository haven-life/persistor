import * as _ from 'underscore';
import { PersistObjectTemplate } from '../PersistObjectTemplate';
import { UtilityFunctions } from '../UtilityFunctions';
import { Database } from './Database';

export namespace Query {
    //PersistObjectTemplate.getFromPersistWithKnexId
    export async function getFromPersistWithKnexId(persistor: typeof PersistObjectTemplate, template, id, cascade, isTransient, idMap, isRefresh, logger, enableChangeTracking?, projection?) {
        const pojos = await getFromPersistWithKnexQuery(persistor, null, template, { _id: id }, cascade, null, null, isTransient, idMap, null, null, isRefresh, logger, enableChangeTracking, projection);
        return pojos[0];
    }

    /**
     * A query is performed which joins the requested entity with any others that have a one-to-one relationship.
     * This yields and array of Pojos that have all of the columns from all of the related entities.
     * These Pojos are processed a template at a time, the processing for which may cause other sub-ordinate
     * entities to be fetched.  All fetches result in promises which ultimately must be resolved on a recursive
     * basis (e.g. keep resolving until there are no more newly added ones).
     *
     * @param {object} requests Array of pending requests
     * @param {object} template super type
     * @param {object/function} queryOrChains mongo style query or function callback
     * @param {object} cascade fetch spec.
     * @param {number} skip offset for the resultset
     * @param {number} limit number of records to return
     * @param {bool} isTransient unknown.
     * @param {object} idMap object mapper for cache
     * @param {object} options order, limit, and skip options
     * @param {object} establishedObject {need to review, used for amorphic}
     * @param {bool} isRefresh {need to review}
     * @param {object} logger object template logger
     * @param {object} enableChangeTracking callback to get the change details
     * @param {object} projection types with property names, will be used to ignore the fields from selects
     * @returns {*}
     */

    // PersistObjectTemplate.getFromPersistWithKnexQuery
    export async function getFromPersistWithKnexQuery(persistor: typeof PersistObjectTemplate, requests, template, queryOrChains, cascade, skip, limit, isTransient, idMap, options, establishedObject, isRefresh, logger, enableChangeTracking?, projection?) {
        let topLevel = !requests;
        requests = requests || [];

        idMap = idMap || {};
        if (!idMap['resolver'])
            idMap['resolver'] = {};

        const promises = [];
        const schema = template.__schema__;
        const results = [];

        const joins = [];

        enableChangeTracking = enableChangeTracking || schema.enableChangeTracking;

        // Determine one-to-one relationships and add function chains for where
        var props = template.getProperties();
        var join = 1;
        for (var prop in props) {
            var defineProperty = props[prop];
            if (UtilityFunctions._persistProperty(props[prop]) && props[prop].type && props[prop].type.__objectTemplate__ && props[prop].type.__table__) {
                // Create the join spec with two keys
                if (!schema || !schema.parents || !schema.parents[prop] || !schema.parents[prop].id) {
                    throw new Error(`${props[prop].type.__name__}.${prop} is missing a parents schema entry`);
                }

                var foreignKey = schema.parents[prop].id;
                var cascadeFetch = (cascade && (typeof (cascade[prop]) != 'undefined')) ? cascade[prop] : null;
                if (((defineProperty['fetch'] && !defineProperty['nojoin']) || cascadeFetch ||
                    (schema.parents[prop].fetch == true && !schema.parents[prop].nojoin)) &&
                    cascadeFetch != false && (!cascadeFetch || !cascadeFetch.nojoin)) {
                    joins.push({
                        prop: prop,
                        template: props[prop].type,
                        parentKey: '_id',
                        childKey: foreignKey,
                        alias: UtilityFunctions.dealias(props[prop].type.__table__) + join++
                    });
                }
            }
        }
        options = options || {}
        if (skip)
            options.offset = skip;
        if (limit)
            options.limit = limit;

        // If at the top level we want to execute this requests and any that are appended during processing
        // Otherwise we are called from within the query results processor and this entire call is already
        // in a request so we just execute it.

        if (topLevel) {
            requests.push(request);
            return await resolveRecursiveRequests(requests, results)
        } else {
            return await request();
        }



        async function getPOJOsFromQuery() {
            return await Database.getPOJOsFromKnexQuery(persistor, template, joins, queryOrChains, options, idMap['resolver'], logger, projection);
        }

        async function resolvePromises() {
            await Promise.all(promises)
            return await results;
        }

        async function getTemplatesFromPOJOS(pojos) {
            joins.forEach((join) => {
                pojos.forEach((pojo) => {
                    pojo.__alias__ = pojo.__alias__ || [];
                    pojo.__alias__.push(join.template);
                });
            });

            var sortMap = {};
            await Promise.all(pojos.map(async (pojo, ix) => {
                sortMap[pojo[`${UtilityFunctions.dealias(template.__table__)}____id`]] = ix;
                const obj = await getTemplateFromKnexPOJO(persistor, pojo, template, requests, idMap, cascade, isTransient, null, establishedObject, null, `${UtilityFunctions.dealias(template.__table__)}___`, joins, isRefresh, logger, enableChangeTracking, projection);
                results[sortMap[obj._id]] = obj;
                promises.push(Promise.resolve(obj));
            }));
        }

        // Request to do entire processing to be executed right now or as part of a request queue
        async function request() {
            const pojos = await getPOJOsFromQuery();
            await getTemplatesFromPOJOS(pojos);
            return await resolvePromises();
        }
    }

    export async function resolveRecursiveRequests(requests: any[], results) {
                const segLength = requests.length;
        const promises = requests.map(async (request, _ix) => {
            const req = await request();
            return req;
        });
        await Promise.all(promises);
        
        await requests.splice(0, segLength);

        //@TODO: Is this really how we deal with requests?
        if (requests.length > 0) {
            return await resolveRecursiveRequests(requests, results);
        }
        else {
            return results;
        }
    }


    /** Enriches a "Plane Old JavaScript Object (POJO)" by creating it using the new Operator
    * so that all prototype information such as functions are created. It will reconstruct
    * references one-to-one and one-two-many by reading them from the database
    *  *
    * @param {object} pojo is the unadorned object
    * @param {obejct} template is the template used to create the object
    * @param {object} requests Array of pending requests
    * @param {object} idMap object mapper for cache
    * @param {object} cascade fetch spec.
    * @param {bool} isTransient unknown.
    * @param {object} defineProperty {@TODO need to check}
    * @param {object} establishedObj {@TODO need to review, used for amorphic}
    * @param {unknown} specificProperties {@TODO need to review}
    * @param {unknown} prefix {@TODO need to review}
    * @param {unknown} joins {@TODO need to review}
    * @param {bool} isRefresh {need to review}
    * @param {object} logger object template logger
    * @param {object} enableChangeTracking callback to get the change details
    * @param {object} projection types with property names, will be used to ignore the fields from selects
    * @returns {*} an object via a promise as though it was created with new template()
    */
    export async function getTemplateFromKnexPOJO(persistor: typeof PersistObjectTemplate, pojo, template, requests, idMap, cascade, isTransient, defineProperty, establishedObj, specificProperties, prefix, joins, isRefresh, logger, enableChangeTracking?, projection?) {
        prefix = prefix || '';

        const prefixId = `${prefix}_id`;
        const prefixTemplate = `${prefix}_template`;
        const prefixVersion = `${prefix}__version__`;

        const usedLogger = logger || persistor.logger;

        usedLogger.debug({
            component: 'persistor',
            module: 'query',
            activity: 'process',
            data: {
                template: template.__name__,
                id: pojo[prefixId],
                persistedTemplate: pojo[prefixTemplate]
            }
        });

        // For recording back refs
        if (!idMap) {
            throw new Error('missing idMap on fromDBPOJO');
        }

        var topLevel = !requests;
        requests = requests || [];

        // In some cases the object we were expecting to populate has changed (refresh case)
        if (pojo && pojo[prefix + '_id'] && establishedObj && establishedObj._id && pojo[prefixId] != establishedObj._id) {
            establishedObj = null;
        }

        // We also get arrays of established objects
        if (establishedObj && establishedObj instanceof Array) {
            establishedObj = _.find(establishedObj, (o) => {
                if (o)
                    return o._id == pojo[prefixId];
                else {
                    usedLogger.debug(
                        {
                            component: 'persistor',
                            module: 'query',
                            activity: 'getTemplateFromKnexPOJO',
                            data: `getTemplateFromKnexPOJO found an empty establishedObj ${template.__name__}`
                        });
                }
            });
        }

        // Create the new object with correct constructor using embedded ID if ObjectTemplate
        if (!establishedObj && !pojo[prefixTemplate]) {
            throw new Error(`Missing _template on ${template.__name__} row ${pojo[prefixId]}`);
        }
        let persistTemplate;
        if (template.__schema__ && template.__schema__.subsetOf) {
            persistTemplate = null;
        }
        else {
            persistTemplate = persistor.__dictionary__[pojo[prefixTemplate]];
        }

        let obj;
        if (establishedObj) {
            obj = establishedObj;
        }
        else if (idMap[pojo[prefixId]]) {
            obj = idMap[pojo[prefixId]];
        }
        else {
            obj = persistor._createEmptyObject(persistTemplate || template, UtilityFunctions.getObjectId(persistor, persistTemplate || template, pojo, prefix), defineProperty, isTransient);
        }

        // Once we find an object already fetched that is not transient query as normal for the rest
        if (!obj.__transient__ && !establishedObj && !isTransient) {
            isTransient = false;
        }

        let schema = obj.__template__.__schema__;
        persistor.withoutChangeTracking(() => {
            obj._id = pojo[prefixId];
            obj._template = pojo[prefixTemplate];
        });

        if (!establishedObj && idMap[obj._id] && allRequiredChildrenAvailableInCache(idMap[obj._id], cascade)) {
            return await idMap[obj._id];
        }

        idMap[obj._id] = obj;

        if (pojo[prefixVersion]) {
            persistor.withoutChangeTracking(() => {
                obj.__version__ = pojo[prefixVersion];
            });
        }

        // Go through all the properties and transfer them to newly created object
        const props = specificProperties || obj.__template__.getProperties();
        let value;

        if (enableChangeTracking) {
            obj.__template__['_ct_enabled_'] = true;
        }

        Object.keys(props).forEach(prop => {
            value = pojo[prefix + prop];
            defineProperty = props[prop];
            const type = defineProperty.type;
            const of = defineProperty.of;
            const cascadeFetch = (cascade && typeof (cascade[prop] != 'undefined')) ? cascade[prop] : null;

            // Create a persistor if not already there
            const persistorPropertyName = `${prop}Persistor`;

            // Make sure this is property is persistent and that it has a value.  We have to skip
            // undefined values in case a new property is added so it can retain it's default value
            if (!UtilityFunctions._persistProperty(defineProperty) || !defineProperty.enumerable) {
                return;
            }

            if (!type) {
                throw new Error(`${obj.__template__.__name__}.${prop} has no type declaration`);
            }

            if (type == Array && of.__table__) {
                if (!schema || !schema.children || !schema.children[prop]) {
                    throw new Error(`${obj.__template__.__name__}.${prop} is missing a children schema entry`);
                }
                if (schema.children[prop].filter && (!schema.children[prop].filter.value || !schema.children[prop].filter.property)) {
                    throw new Error(`Incorrect filter properties on ${prop} in ${obj.__template__.__name__}`);
                }

                if ((defineProperty['fetch'] || cascadeFetch || schema.children[prop].fetch) && cascadeFetch != false && !obj[persistorPropertyName].isFetching) {
                    queueChildrenLoadRequest(obj, prop, schema, defineProperty, projection, persistorPropertyName, cascadeFetch);
                }
                else {
                    updatePersistorProp(obj, persistorPropertyName, { isFetched: false });
                }
            }
            else if (type.isObjectTemplate && (schema || obj[prop] && obj[prop]._id)) {
                //var foreignId = (establishedObj && obj[prop]) ? obj[prop]._id : null;
                if (!obj[prop]) {
                    persistor.withoutChangeTracking(() => {
                        obj[prop] = null;
                    });
                }

                // Determine the id needed
                if (!schema || !schema.parents || !schema.parents[prop] || !schema.parents[prop].id) {
                    throw new Error(`${obj.__template__.__name__}.${prop} is missing a parents schema entry`);
                }

                const foreignKey = schema.parents[prop].id;
                const foreignId = pojo[prefix + foreignKey] || (obj[persistorPropertyName] ? obj[persistorPropertyName].id : '') || '';

                if (enableChangeTracking) {
                    obj[`_ct_org_${prop}`] = foreignId;
                }

                // Return copy if already there
                const cachedObject = idMap[foreignId];
                if (cachedObject && (!cascadeFetch || !cascadeFetch.fetch || allRequiredChildrenAvailableInCache(cachedObject, cascadeFetch.fetch))) {
                    if (!obj[prop] || obj[prop].__id__ != cachedObject.__id__) {
                        persistor.withoutChangeTracking(() => {
                            obj[prop] = cachedObject;
                        });
                        updatePersistorProp(obj, persistorPropertyName, { isFetched: true, id: foreignId });
                    }
                }
                else {
                    if ((defineProperty['fetch'] || cascadeFetch || schema.parents[prop].fetch) && cascadeFetch != false && !obj[persistorPropertyName].isFetching) {
                        if (foreignId) {
                            queueLoadRequest(obj, prop, schema, defineProperty, cascadeFetch, persistorPropertyName, foreignId, enableChangeTracking, projection);
                        } else {
                            updatePersistorProp(obj, persistorPropertyName, { isFetched: true, id: foreignId })
                        }
                    } else {
                        updatePersistorProp(obj, persistorPropertyName, { isFetched: false, id: foreignId })
                    }
                }
            }
            else {
                if (typeof (pojo[prefix + prop]) != 'undefined') {
                    value = pojo[prefix + prop];
                    persistor.withoutChangeTracking(() => {
                        if (type == Date) {
                            obj[prop] = value ? new Date(value * 1) : null;
                        }
                        else if (type == Number) {
                            obj[prop] = (!value && value !== 0) ? null : value * 1;
                        }
                        else if (type == Object || type == Array) {
                            try {
                                obj[prop] = value ? JSON.parse(value) : null;
                            } catch (e) {
                                usedLogger.debug({
                                    component: 'persistor', module: 'query', activity: 'getTemplateFromKnexPOJO',
                                    data: `Error retrieving ${obj.__id__}.${prop} -- ${e.message}`
                                });
                                obj[prop] = null;
                            }
                        }
                        else {
                            obj[prop] = value;
                        }
                        if (enableChangeTracking) {
                            obj['_ct_org_' + prop] = obj[prop];
                        }
                    });
                }
            }
        });
        if (topLevel)
            return await resolveRecursiveRequests(requests, obj)
        else
            return await obj;

        function collectLikeFilters(_prop, _query, thisDefineProperty, foreignFilterKey) {

            // Collect a structure of similar filters (excluding the first one)
            let filters = null;
            let excluded = 0; // Exclude first

            for (const candidateProp in props) {
                const candidateDefineProp = props[candidateProp];
                const filter = schema.children[candidateProp] ? schema.children[candidateProp].filter : null;
                if (filter && filter.property == foreignFilterKey &&
                    candidateDefineProp.of.__table__ == thisDefineProperty.of.__table__ && excluded++) {
                    filters = filters || {};
                    filters[candidateProp] = {
                        foreignFilterKey: filter.property,
                        foreignFilterValue: filter.value,
                    }
                }
            }

            return filters;
        }

        function buildFilterQuery(query, foreignFilterKey, foreignFilterValue, alternateProps) {
            if (alternateProps) {
                query['$or'] = _.map(alternateProps, (prop: any) => {
                    const condition = {}
                    condition[prop.foreignFilterKey] = prop.foreignFilterValue;
                    return condition;
                });

                const condition = {}
                condition[foreignFilterKey] = foreignFilterValue;

                query['$or'].push(condition);
            } else
                query[foreignFilterKey] = foreignFilterValue;
        }

        function updatePersistorProp(obj, prop, values) {
            persistor.withoutChangeTracking(() => {
                values['isFetching'] = false;

                if (!obj[prop]) {
                    obj[prop] = {};
                }
                var modified = false;
                _.map(values, (value, key) => {
                    if (obj[prop][key] != value) {
                        obj[prop][key] = value;
                        modified = true;
                    }
                });
                if (modified) {
                    const tempProps = obj[prop];
                    obj[prop] = null;
                    obj[prop] = tempProps;
                }
            });
        }

        function queueLoadRequest(obj, prop, schema, defineProperty, cascadeFetch, persistorPropertyName, foreignId, enableChangeTracking, projection) {
            const query = { _id: foreignId };
            const options = {};
            const closureProp = prop;
            const closurePersistorProp = persistorPropertyName;
            const closureCascade = UtilityFunctions.processCascade(query, options, cascadeFetch, (schema && schema.parents) ? schema.parents[prop].fetch : null, defineProperty.fetch);
            const closureForeignId = foreignId;
            const closureType = defineProperty.type;
            const closureDefineProperty = defineProperty;

            const join = _.find(joins, (j: any) => { return j.prop == prop });

            requests.push(generateQueryRequest());

            async function generateQueryRequest() {
                if (join) {
                    if (pojo[`${join.alias}____id`]) {
                        await getTemplateFromKnexPOJO(persistor, pojo, closureType, requests, idMap, closureCascade, isTransient, closureDefineProperty, obj[closureProp], null, join.alias + '___', null, isRefresh, logger, enableChangeTracking, projection)
                    }
                }
                else {
                    await getFromPersistWithKnexQuery(persistor, requests, closureType, query, closureCascade, null, null, isTransient, idMap, {}, obj[closureProp], isRefresh, logger, enableChangeTracking, projection);
                }

                persistor.withoutChangeTracking(() => {
                    obj[closurePersistorProp].isFetching = true;
                });

                persistor.withoutChangeTracking(() => {
                    obj[closureProp] = idMap[closureForeignId];
                });

                if (obj[closurePersistorProp]) {
                    updatePersistorProp(obj, closurePersistorProp, { isFetched: true, id: closureForeignId });
                }
            }

        }

        function queueChildrenLoadRequest(obj, prop, schema, defineProperty, projection, persistorPropertyName, cascadeFetch) {

            const foreignFilterKey = schema.children[prop].filter ? schema.children[prop].filter.property : null;
            const foreignFilterValue = schema.children[prop].filter ? schema.children[prop].filter.value : null;

            // Construct foreign key query
            const query = {};
            const options = defineProperty.queryOptions || { sort: { _id: 1 } };
            const limit = options.limit || null;
            let alternateProps;

            query[schema.children[prop].id] = obj._id;

            if (foreignFilterKey) {
                // accumulate hash of all like properties (except the first one)
                alternateProps = collectLikeFilters(prop, query, defineProperty, foreignFilterKey);
                // If other than the first one just leave it for the original to take care of
                if (alternateProps && alternateProps[prop])
                    return;
                else
                    buildFilterQuery(query, foreignFilterKey, foreignFilterValue, alternateProps);
            }

            // Handle
            const closureOf = defineProperty.of;
            const closureProp = prop;
            const closurePersistorProp = persistorPropertyName
            const closureCascade = UtilityFunctions.processCascade(query, options, cascadeFetch, (schema && schema.children) ? schema.children[prop].fetch : null, defineProperty.fetch);

            // Fetch sub-ordinate entities and convert to objects
            persistor.withoutChangeTracking(() => {
                obj[persistorPropertyName].isFetching = true;
            });

            requests.push(async () => {
                const objs = await getFromPersistWithKnexQuery(persistor, requests, closureOf, query, closureCascade, null, limit, isTransient, idMap, options, obj[closureProp], isRefresh, logger, null, projection);
                persistor.withoutChangeTracking(() => {
                    if (foreignFilterKey) {
                        obj[closureProp] = _.filter(objs, function (obj) {
                            return obj[foreignFilterKey] == foreignFilterValue;
                        });
                        if (alternateProps)
                            _.each(alternateProps, (alternateProp: any, alternatePropKey) => {
                                obj[alternatePropKey] = _.filter(objs, (obj) => {
                                    return obj[alternateProp.foreignFilterKey] == alternateProp.foreignFilterValue
                                })
                            })
                    } else {
                        obj[closureProp] = objs;
                    }
                });
                var start = options ? options.start || 0 : 0;
                updatePersistorProp(obj, closurePersistorProp, { isFetched: true, start: start, next: start + objs.length });
            });
        }

        function allRequiredChildrenAvailableInCache(cachedObject, fetchSpec) {
            return Object.keys(fetchSpec).reduce((loaded, currentObj) => {
                if (loaded) {
                    if (!fetchSpec[currentObj] || (cachedObject[`${currentObj}Persistor`] && cachedObject[`${currentObj}Persistor`].isFetched)) {
                        return true
                    }
                    else {
                        return false;
                    }
                }
                else {
                    return false;
                }
            }, true);
        }
    }

}