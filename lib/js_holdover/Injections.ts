import { PersistObjectTemplate } from "../PersistObjectTemplate";
import { Persistent } from '../Persistent';

export namespace Injections {
    export function _injectTemplateFunctions(template) {
        template.getFromPersistWithId = Persistent.getFromPersistWithId.bind(template);
        template.getFromPersistWithQuery = Persistent.getFromPersistWithQuery.bind(template);
        template.deleteFromPersistWithQuery = Persistent.deleteFromPersistWithQuery.bind(template);
        template.persistorFetchById = Persistent.persistorFetchById.bind(template);
        template.persistorDeleteByQuery = Persistent.persistorDeleteByQuery.bind(template);
        template.persistorFetchByQuery = Persistent.persistorFetchByQuery.bind(template);
        template.persistorCountByQuery = Persistent.persistorCountByQuery.bind(template);
        template.deleteFromPersistWithId = Persistent.deleteFromPersistWithId.bind(template);
        template.countFromPersistWithQuery = Persistent.countFromPersistWithQuery.bind(template);
        template.persistorIsKnex = Persistent.persistorIsKnex.bind(template);
        template.persistorGetKnex = Persistent.persistorGetKnex.bind(template);
        template.persistorGetTableName = Persistent.persistorGetTableName.bind(template);
        template.persistorGetParentKey = Persistent.persistorGetParentKey.bind(template);
        template.persistorGetChildKey = Persistent.persistorGetChildKey.bind(template);
        template.persistorGetId = Persistent.persistorGetId.bind(template);
        template.persistorKnexParentJoin = Persistent.persistorKnexParentJoin.bind(template);
        template.persistorKnexChildJoin = Persistent.persistorKnexChildJoin.bind(template);
        template.isKnex = template.persistorIsKnex;
        template.getKnex = template.persistorGetKnex;
        template.getTableName = template.persistorGetTableName;
        template.getParentKey = template.persistorGetParentKey;
        template.getChildKey = template.persistorGetChildKey;
        template.getPrimaryKey = template.persistorGetId;
        template.knexParentJoin = template.persistorKnexParentJoin;
        template.knexChildJoin = template.persistorKnexChildJoin;
        template._injectProperties = Persistent._injectProperties.bind(template);
    }
    export function _injectObjectFunctions(template) {

        template.prototype.persistSave = Persistent.prototype.persistSave;
        template.prototype.persistTouch = Persistent.prototype.persistTouch;
        template.prototype.persistDelete = Persistent.prototype.persistDelete;
        template.prototype.setDirty = Persistent.prototype.setDirty;
        template.prototype.setAsDeleted = Persistent.prototype.setAsDeleted;
        template.prototype.cascadeSave = Persistent.prototype.cascadeSave;
        template.prototype.isStale = Persistent.prototype.isStale;
        template.prototype.persistorIsStale = Persistent.prototype.persistorIsStale;
        template.prototype.fetchProperty = Persistent.prototype.fetchProperty;
        template.prototype.fetch = Persistent.prototype.fetch;
        template.prototype.persistorFetchReferences  = Persistent.prototype.persistorFetchReferences;
        template.prototype.persistorRefresh = Persistent.prototype.persistorRefresh;
        template.prototype.persistorSave = Persistent.prototype.persistorSave;
        template.prototype.persistorDelete = Persistent.prototype.persistorDelete;

        if (template.defineProperties && typeof(template._injectProperties) == 'function') {
            template._injectProperties();
        }
    };
}