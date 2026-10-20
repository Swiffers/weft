// Which instances the contextual bar acts on, remembered for the session.
//
// The person picks the shared ones (the default), one instance, or every
// instance once, and every later selection starts on that pick, until the
// webview goes away. Nothing outlives the webview, so a pick is always
// read by the same code that wrote it. Kept per project: instance ids are the program's
// own, so another project's pick means nothing here.

import type { InstanceTarget } from '../../protocol';

export class InstanceTargetMemory {
	private picks = new Map<string, InstanceTarget>();

	/// The pick for `project`; `undefined` is the shared ones.
	get(project: string): InstanceTarget | undefined {
		return this.picks.get(project);
	}

	set(project: string, target: InstanceTarget | undefined): void {
		if (target === undefined) this.picks.delete(project);
		else this.picks.set(project, target);
	}
}

/// The one memory the webview keeps, alive as long as it is.
export const sessionInstanceTargets = new InstanceTargetMemory();
