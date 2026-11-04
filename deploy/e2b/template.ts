import { Template } from "e2b";

// Bugnet runs every command as root, so root is the default user and its home the workdir
export const template = Template().fromBaseImage().setUser("root").setWorkdir("/root");
