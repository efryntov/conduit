package ca.psiphon.conduit.state;

import ca.psiphon.conduit.state.IConduitStateCallback;

// Interface to register for conduit state updates
// IMPORTANT: to keep the interface backwards compatible, new methods should be added to the end of the interface
interface IConduitStateService {
    void registerClient(IConduitStateCallback callback);
    void unregisterClient(IConduitStateCallback callback);
    // Register the client with the JSON schema version to be used for the state updates
    // via the IConduitStateCallback.onStateUpdate method
    void registerClientWithSchemaVersion(IConduitStateCallback callback, int schemaVersion);
    // A simple method to fetch the Conduit private key
    String fetchConduitPrivateKey();
}
