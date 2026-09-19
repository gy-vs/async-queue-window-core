import {test} from 'node:test';
import assert from 'node:assert';
import delay from 'delay';
import PQueue from '../source/index.js';

test('rate-limit rapid pause/start cycles', async () => {
	const queue = new PQueue({
		interval: 100,
		intervalCap: 1,
		autoStart: false,
	});

	const results: number[] = [];
	const promises: Array<Promise<number>> = [];

	// Add tasks
	for (let i = 0; i < 4; i++) {
		promises.push(queue.add(async () => {
			results.push(i);
			return i;
		}));
	}

	// Rapid start/pause cycles
	const startPauseCycles = async () => {
		for (let cycle = 0; cycle < 3; cycle++) {
			queue.start();
			// eslint-disable-next-line no-await-in-loop
			await delay(25); // Let one task run
			queue.pause();
			// eslint-disable-next-line no-await-in-loop
			await delay(50); // Pause during interval
		}
	};

	await startPauseCycles();

	// Finally let everything run
	queue.start();
	await Promise.all(promises);

	// All tasks should complete despite rapid state changes
	assert.equal(results.length, 4);
});

test('rate-limit edge case with zero-interval', async () => {
	// Zero interval should effectively disable rate limiting
	const queue = new PQueue({
		interval: 0,
		intervalCap: 1,
	});

	const startTime = Date.now();
	await Promise.all([
		queue.add(async () => delay(10)),
		queue.add(async () => delay(10)),
		queue.add(async () => delay(10)),
	]);
	const elapsed = Date.now() - startTime;

	// Should run concurrently, not rate-limited
	assert.ok(elapsed < 50, 'Tasks should run without rate limiting');
});

test('rate-limit state consistency with sync microtask scheduling', async () => {
	const queue = new PQueue({
		interval: 100,
		intervalCap: 1,
	});

	const events: string[] = [];
	queue.on('rateLimit', () => events.push('rateLimit'));
	queue.on('rateLimitCleared', () => events.push('rateLimitCleared'));

	// Schedule multiple tasks synchronously
	const promises = [];
	for (let i = 0; i < 3; i++) {
		promises.push(queue.add(async () => {
			// Immediate microtask
			await Promise.resolve();
			return i;
		}));
	}

	// Events should be consistent
	await delay(10);
	assert.equal(queue.isRateLimited, true);
	assert.equal(events[0], 'rateLimit');

	await Promise.all(promises);
	assert.equal(queue.isRateLimited, false);
	assert.ok(events.includes('rateLimitCleared'));
});

test('rate-limit with queue manipulation during rate-limit event', async () => {
	const queue = new PQueue({
		interval: 100,
		intervalCap: 1,
	});

	let manipulated = false;

	queue.on('rateLimit', () => {
		if (!manipulated) {
			manipulated = true;
			// Try to manipulate queue during event
			queue.add(async () => 'extra');
			queue.pause();
			queue.start();
		}
	});

	// Add tasks to trigger rate limit
	const results = await Promise.all([
		queue.add(async () => {
			await delay(10);
			return 1;
		}),
		queue.add(async () => {
			await delay(10);
			return 2;
		}),
		queue.add(async () => {
			await delay(10);
			return 3;
		}),
	]);

	// Queue should remain stable despite manipulation
	assert.ok(results.includes(1));
	assert.ok(results.includes(2));
	assert.ok(results.includes(3));
});

test('onRateLimit() with microtask race condition', async () => {
	const queue = new PQueue({
		interval: 100,
		intervalCap: 1,
	});

	const promises = [];
	let rateLimitCalled = false;

	// Race: onRateLimit vs task completion
	promises.push(queue.add(async () => {
		await delay(10);
		return 'first';
	}));

	for (const promise of [queue.add(async () => 'second')]) {
		promises.push(promise);
	}

	// Try to attach listener after tasks are queued
	await Promise.resolve(); // Microtask delay
	const rateLimitPromise = (async () => {
		await queue.onRateLimit();
		rateLimitCalled = true;
	})();

	await Promise.all([...promises, rateLimitPromise]);
	assert.ok(rateLimitCalled, 'onRateLimit should handle late attachment');
});

test('onRateLimitCleared() with microtask race condition', async () => {
	const queue = new PQueue({
		interval: 100,
		intervalCap: 1,
	});

	// Trigger rate limit
	queue.add(async () => delay(10));
	queue.add(async () => delay(10));

	await delay(20); // Let rate limit trigger

	// Wait for clear using the promise API
	const clearedPromise = queue.onRateLimitCleared();

	// Wait for clear
	await queue.onIdle();
	await delay(110); // Past interval

	// The promise should resolve when rate limit is cleared
	await clearedPromise;
	assert.ok(true, 'onRateLimitCleared should handle attachment during rate limit');
});

test('onRateLimit() called during state transition', async () => {
	const queue = new PQueue({
		interval: 100,
		intervalCap: 1,
	});

	const sequence: string[] = [];

	queue.add(async () => {
		sequence.push('task1-start');
		await delay(10);
		sequence.push('task1-end');
	});

	const secondTask = queue.add(async () => {
		sequence.push('task2');
	});

	// Wait for rate limit to be triggered
	await queue.onRateLimit();
	sequence.push('rate-limit');

	// Wait for second task to complete
	await secondTask;

	await queue.onIdle();

	// Verify sequence order
	assert.equal(sequence[0], 'task1-start');
	assert.ok(sequence.includes('rate-limit'));
});

test('onRateLimit/onRateLimitCleared rapid transitions', async () => {
	const queue = new PQueue({
		interval: 50,
		intervalCap: 1,
	});

	const events: string[] = [];

	queue.on('rateLimit', () => events.push('limited'));
	queue.on('rateLimitCleared', () => events.push('cleared'));

	// Create rapid transitions
	const createTransitions = async () => {
		for (let i = 0; i < 3; i++) {
			queue.add(async () => delay(10));
			queue.add(async () => delay(10));
			// eslint-disable-next-line no-await-in-loop
			await delay(60); // Wait for interval reset
			// eslint-disable-next-line no-await-in-loop
			await queue.onIdle();
		}
	};

	await createTransitions();

	// Should have alternating events
	assert.ok(events.length >= 3, 'Should have multiple rate limit events');
	assert.ok(events.includes('limited'));
	assert.ok(events.includes('cleared'));
});

test('onRateLimit() resolves when rate limit is triggered', async () => {
	const queue = new PQueue({
		interval: 100,
		intervalCap: 1,
	});

	// Add tasks to eventually trigger rate limit
	queue.add(async () => delay(10));

	let rateLimitResolved = false;
	const rateLimitPromise = (async () => {
		await queue.onRateLimit();
		rateLimitResolved = true;
	})();

	// Add another task which will trigger rate limit
	queue.add(async () => delay(10));

	// Give time for rate limit to be triggered
	await delay(20);

	// OnRateLimit should have resolved since we hit rate limit
	assert.ok(rateLimitResolved, 'onRateLimit should resolve when rate limit is triggered');

	// Clean up
	await queue.onIdle();
});

test('fixed window restores full quota after crossing the boundary while idle', async () => {
	const queue = new PQueue({
		intervalCap: 2,
		interval: 300,
	});

	const t0 = Date.now();
	const starts: number[] = [];
	const stamp = () => {
		starts.push(Date.now() - t0);
	};

	// Spread the window's quota over the window: ~0ms and ~200ms of window [0, 300).
	await queue.add(async () => {
		stamp();
	});
	await delay(200);
	await queue.add(async () => {
		stamp();
	});

	// Queue is briefly idle; let the window boundary pass.
	await delay(150);

	// The new window must immediately offer its full quota to both tasks.
	const t1 = Date.now();
	await queue.add(async () => {
		stamp();
	});
	await queue.add(async () => {
		stamp();
	});
	const elapsed = Date.now() - t1;
	assert.ok(elapsed < 60, `tasks after the boundary should run immediately, took ${elapsed}ms`);

	// The next task exceeds the cap and has to wait for the following window.
	let thirdStarted = false;
	const third = queue.add(async () => {
		thirdStarted = true;
		stamp();
	});

	await delay(150);
	assert.ok(!thirdStarted, 'task beyond the cap should wait for the next window');

	await third;
	const windowWait = starts[4]! - starts[2]!;
	assert.ok(windowWait >= 200, `task beyond the cap should wait for the next window, waited ${windowWait}ms`);
});

test('re-enqueue before the window boundary is still limited by the current window', async () => {
	const queue = new PQueue({
		intervalCap: 2,
		interval: 200,
	});

	// Use up the window's quota.
	await queue.add(async () => '🧜‍♂️');
	await queue.add(async () => '🧜‍♂️');

	// Re-enqueue while still inside the same window.
	await delay(60);

	const t1 = Date.now();
	let thirdStart = -1;
	const third = queue.add(async () => {
		thirdStart = Date.now() - t1;
	});

	await delay(60);
	assert.equal(thirdStart, -1, 'task must wait for the current window to end');

	await third;
	assert.ok(thirdStart >= 100, `task should wait for the window boundary, waited ${thirdStart}ms`);
	assert.ok(thirdStart < 190, `task should run at the window boundary, waited ${thirdStart}ms`);
});

test('remaining quota is remembered when re-enqueueing before the window boundary', async () => {
	const queue = new PQueue({
		intervalCap: 2,
		interval: 200,
	});

	// Consume only one of the two slots, then let the queue go idle.
	await queue.add(async () => '🧜‍♂️');

	await delay(60);

	// The remembered count allows exactly one more task in this window,
	// so the count must not be reset just because the queue went idle.
	const t1 = Date.now();
	let secondStart = -1;
	await queue.add(async () => {
		secondStart = Date.now() - t1;
	});
	assert.ok(secondStart >= 0 && secondStart < 60, `remaining slot should be usable immediately, took ${secondStart}ms`);

	let thirdStart = -1;
	const third = queue.add(async () => {
		thirdStart = Date.now() - t1;
	});

	await delay(60);
	assert.equal(thirdStart, -1, 'cap of the current window must still be enforced');

	await third;
	assert.ok(thirdStart >= 100, `third task should wait for the window boundary, waited ${thirdStart}ms`);
});

test('re-entrant add from an active listener after the window expired', async () => {
	const queue = new PQueue({
		intervalCap: 2,
		interval: 200,
	});

	// Use up the window's quota and let the queue go idle.
	await queue.add(async () => '🧜‍♂️');
	await queue.add(async () => '🧜‍♂️');

	// Cross the window boundary.
	await delay(250);

	let aStart = -1;
	let bStart = -1;
	let cStart = -1;

	queue.once('active', () => {
		// Re-entrant adds while the first task of the new window is starting.
		queue.add(async () => {
			bStart = Date.now();
		});
		queue.add(async () => {
			cStart = Date.now();
		});
	});

	const t1 = Date.now();
	await queue.add(async () => {
		aStart = Date.now();
	});

	// The fresh window's full quota covers A and the re-entrant B, but not C.
	assert.ok(aStart >= 0 && aStart - t1 < 60, `first task of the new window should run immediately, took ${aStart - t1}ms`);
	assert.ok(bStart >= 0 && bStart - t1 < 60, `re-entrant task should share the new window, took ${bStart - t1}ms`);
	assert.equal(cStart, -1, 're-entrant task beyond the cap must wait for the next window');

	await delay(300);
	assert.ok(cStart - aStart >= 150, `re-entrant task beyond the cap should wait for the next window, waited ${cStart - aStart}ms`);
});

test('carryoverIntervalCount carries pending tasks across the window boundary', async () => {
	const queue = new PQueue({
		intervalCap: 2,
		interval: 200,
		carryoverIntervalCount: true,
	});

	const t0 = Date.now();
	let thirdStart = -1;

	// Two long tasks occupy the window and stay pending across the boundary.
	queue.add(async () => delay(300));
	queue.add(async () => delay(300));
	// Queued behind the cap.
	const third = queue.add(async () => {
		thirdStart = Date.now() - t0;
	});

	// At the first boundary tick the count resets to the pending count (2),
	// so the queued task must keep waiting for a window with free quota.
	await delay(250);
	assert.equal(thirdStart, -1, 'pending tasks carry over into the next window');

	await third;
	assert.ok(thirdStart >= 300, `third task should wait for a window with free quota, started at ${thirdStart}ms`);
});

test('carryoverIntervalCount counts pending tasks when resetting an expired window', async () => {
	const queue = new PQueue({
		intervalCap: 2,
		interval: 200,
		carryoverIntervalCount: true,
	});

	const t0 = Date.now();
	const starts: number[] = [];
	const stamp = () => {
		starts.push(Date.now() - t0);
	};

	// A long task keeps `pending` while the queue empties and the window expires.
	const long = queue.add(async () => {
		stamp();
		await delay(400);
	});

	// Cross the boundary of the expired window [0, 200).
	await delay(250);

	// The reset carries the pending task over: only one free slot remains.
	const t1 = Date.now();
	await queue.add(async () => {
		stamp();
	});
	assert.ok(Date.now() - t1 < 60, 'remaining slot should be usable immediately');

	let thirdStarted = false;
	const third = queue.add(async () => {
		thirdStarted = true;
		stamp();
	});

	await delay(80);
	assert.ok(!thirdStarted, 'carried-over pending count must consume quota');

	await third;
	await long;
	const windowWait = starts[2]! - starts[1]!;
	assert.ok(windowWait >= 100, `third task should wait for the next window, waited ${windowWait}ms`);
});
