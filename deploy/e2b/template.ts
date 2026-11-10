import { Template } from "e2b";

// imageRef is <REGISTRY>@sha256:<digest>, which build.ts reads from IMAGE_REF and checks
export function templateFor(imageRef: string) {
    return Template()
        .fromImage(imageRef)
        .runCmd("/usr/local/lib/bugnet/setup-image.sh finish --target e2b")
        .setUser("root")
        .setWorkdir("/root");
}
