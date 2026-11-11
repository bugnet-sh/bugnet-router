import { Template } from "e2b";

// imageRef is <REGISTRY>@sha256:<digest>, which build.ts reads from IMAGE_REF and checks
export function templateFor(imageRef: string) {
    return Template()
        .fromImage(imageRef)
        // The image's default user is E2B's own, and finish needs root
        .runCmd("/usr/local/lib/bugnet/setup-image.sh finish --target e2b", { user: "root" })
        .setUser("root")
        .setWorkdir("/root");
}
