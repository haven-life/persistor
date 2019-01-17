import { PersistObjectTemplate } from "./PersistObjectTemplate";

export namespace Mongo {

    /**
     *  savePojoToMongo
     */
    export function save(persistor: typeof PersistObjectTemplate, obj, pojo, updateId, _txn, logger) {
        const usedLogger = logger || this.logger;
        usedLogger.debug({
            component: 'persistor',
            module: 'db',
            activity: 'write'
        },
        `Saving ${obj.__template__.__name__} to ${obj.__template__.__collection__}`);

        const origVer = obj.__version__;
        obj.__version__ = obj.__version__ ? obj.__version__ + 1 : 1;
        pojo.__version__ = obj.__version__;

    }
    
}