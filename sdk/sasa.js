/**
 * SASA Analytics SDK  v1.0.0
 * Drop-in analytics for any frontend deployment.
 *
 * Usage — paste ONE script tag into your HTML <head>:
 *
 *   <script
 *     src="https://your-server/sdk/sasa.js"
 *     data-project="my-app"
 *     data-api-key="YOUR_KEY"
 *     data-track-videos="true"
 *     data-track-clicks="true"
 *     data-track-scroll="true">
 *   </script>
 *
 * Manual tracking:
 *   SASA.track('button_click', { label: 'Sign Up' });
 *   SASA.identify('user_123', { plan: 'pro' });
 *   SASA.page('Pricing');        // manual page name override
 */
(function (global) {
  'use strict';

  // ── read config from the script tag that loaded us ──────────────────────
  var scriptTag = document.currentScript ||
    (function () {
      var scripts = document.getElementsByTagName('script');
      return scripts[scripts.length - 1];
    })();

  function attr(name, fallback) {
    return (scriptTag && scriptTag.getAttribute('data-' + name)) || fallback;
  }

  var CONFIG = {
    apiBase:      attr('api-base',    (location.protocol + '//' + location.hostname + ':8000')),
    project:      attr('project',     'default'),
    apiKey:       attr('api-key',     ''),
    trackVideos:  attr('track-videos','true') !== 'false',
    trackClicks:  attr('track-clicks','true') !== 'false',
    trackScroll:  attr('track-scroll','true') !== 'false',
    batchInterval: parseInt(attr('batch-interval', '2000'), 10),
    debug:        attr('debug', 'false') === 'true',
  };

  // ── persistent IDs ───────────────────────────────────────────────────────
  function getOrCreate(key) {
    try {
      var val = localStorage.getItem(key);
      if (!val) { val = uuid(); localStorage.setItem(key, val); }
      return val;
    } catch (e) { return uuid(); }
  }

  var SESSION_KEY = 'sf_session_' + CONFIG.project;
  var USER_KEY    = 'sf_user_'    + CONFIG.project;

  // New session if tab was closed for >30 min
  function freshSession() {
    try {
      var last = parseInt(localStorage.getItem('sf_last_active_' + CONFIG.project) || '0', 10);
      if (Date.now() - last > 30 * 60 * 1000) {
        var sid = uuid();
        localStorage.setItem(SESSION_KEY, sid);
        return sid;
      }
    } catch (e) {}
    return getOrCreate(SESSION_KEY);
  }

  var state = {
    sessionId:    freshSession(),
    userId:       getOrCreate(USER_KEY),
    traits:       {},
    pageStart:    Date.now(),
    scrollDepth:  0,
    queue:        [],
    flushTimer:   null,
    videoSessions:{},   // element → session_id
  };

  // ── utils ────────────────────────────────────────────────────────────────
  function uuid() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
      var r = Math.random() * 16 | 0;
      return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
    });
  }

  function log() {
    if (CONFIG.debug) console.log('[SASA]', ...arguments);
  }

  function now() { return Date.now(); }

  // ── event construction ───────────────────────────────────────────────────
  function buildEvent(eventName, properties) {
    properties = properties || {};
    return {
      event_id:   uuid(),
      project:    CONFIG.project,
      api_key:    CONFIG.apiKey,
      session_id: state.sessionId,
      user_id:    state.userId,
      event_name: eventName,
      url:        location.href,
      path:       location.pathname,
      referrer:   document.referrer || '',
      title:      document.title,
      screen_w:   screen.width,
      screen_h:   screen.height,
      user_agent: navigator.userAgent,
      timestamp:  Date.now() / 1000,
      properties: Object.assign({}, state.traits, properties),
    };
  }

  // ── batched send ─────────────────────────────────────────────────────────
  function enqueue(event) {
    state.queue.push(event);
    log('enqueued', event.event_name, event.properties);
    if (!state.flushTimer) {
      state.flushTimer = setTimeout(flush, CONFIG.batchInterval);
    }
  }

  function flush() {
    state.flushTimer = null;
    if (!state.queue.length) return;
    var batch = state.queue.splice(0);
    var payload = JSON.stringify({ events: batch });

    // Beacon API for page-unload, fetch otherwise
    if (navigator.sendBeacon) {
      navigator.sendBeacon(CONFIG.apiBase + '/ingest/batch', payload);
    } else {
      fetch(CONFIG.apiBase + '/ingest/batch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: payload,
        keepalive: true,
      }).catch(function () {});
    }
  }

  // ── public API ───────────────────────────────────────────────────────────
  var SASA = {
    /** Track a custom event. */
    track: function (eventName, properties) {
      enqueue(buildEvent(eventName, properties));
    },

    /** Attach user identity to all future events. */
    identify: function (userId, traits) {
      state.userId = userId;
      state.traits = Object.assign(state.traits, traits || {});
      try { localStorage.setItem(USER_KEY, userId); } catch (e) {}
      enqueue(buildEvent('identify', traits || {}));
    },

    /** Manually record a page view with an optional name override. */
    page: function (name, properties) {
      enqueue(buildEvent('page_view', Object.assign({ page_name: name || document.title }, properties || {})));
    },

    config: CONFIG,
  };

  // ── auto: page view ──────────────────────────────────────────────────────
  function trackPageView() {
    state.pageStart = now();
    SASA.track('page_view', { page_name: document.title, path: location.pathname });
  }

  // SPA support — intercept pushState / replaceState
  (function () {
    function wrap(method) {
      var orig = history[method];
      history[method] = function () {
        orig.apply(history, arguments);
        trackPageView();
      };
    }
    wrap('pushState');
    wrap('replaceState');
    window.addEventListener('popstate', trackPageView);
  })();

  // ── auto: session keep-alive ─────────────────────────────────────────────
  setInterval(function () {
    try { localStorage.setItem('sf_last_active_' + CONFIG.project, String(Date.now())); } catch (e) {}
  }, 10000);

  // ── auto: page time + flush on unload ────────────────────────────────────
  window.addEventListener('visibilitychange', function () {
    if (document.visibilityState === 'hidden') {
      SASA.track('page_hidden', { time_on_page_ms: now() - state.pageStart });
      flush();
    }
  });
  window.addEventListener('beforeunload', function () {
    SASA.track('page_exit', { time_on_page_ms: now() - state.pageStart, scroll_depth: state.scrollDepth });
    flush();
  });

  // ── auto: scroll depth ───────────────────────────────────────────────────
  if (CONFIG.trackScroll) {
    var scrollTimer;
    window.addEventListener('scroll', function () {
      clearTimeout(scrollTimer);
      scrollTimer = setTimeout(function () {
        var doc = document.documentElement;
        var pct = Math.round((window.scrollY + window.innerHeight) / doc.scrollHeight * 100);
        if (pct > state.scrollDepth) {
          state.scrollDepth = pct;
          // fire milestones
          [25, 50, 75, 90, 100].forEach(function (m) {
            if (pct >= m && state.scrollDepth >= m) {
              SASA.track('scroll_depth', { depth: m });
            }
          });
        }
      }, 200);
    }, { passive: true });
  }

  // ── auto: click tracking ──────────────────────────────────────────────────
  if (CONFIG.trackClicks) {
    document.addEventListener('click', function (e) {
      var el = e.target;
      // walk up to find meaningful element
      for (var i = 0; i < 5 && el; i++) {
        var tag = el.tagName || '';
        if (tag === 'A' || tag === 'BUTTON' || el.getAttribute('role') === 'button' ||
            el.getAttribute('data-sf-track')) {
          SASA.track('click', {
            element:  tag.toLowerCase(),
            text:     (el.innerText || '').slice(0, 100).trim(),
            href:     el.href || '',
            id:       el.id || '',
            class:    (el.className || '').slice(0, 80),
            sf_label: el.getAttribute('data-sf-label') || '',
          });
          break;
        }
        el = el.parentElement;
      }
    }, true);
  }

  // ── auto: video tracking ─────────────────────────────────────────────────
  if (CONFIG.trackVideos) {
    function attachVideo(video) {
      if (video._sf_attached) return;
      video._sf_attached = true;

      var vsid = uuid();
      var videoId = video.id || video.src || ('video_' + Math.random().toString(36).slice(2,7));
      var title   = video.title || video.getAttribute('data-title') || videoId;
      var buffering = false;

      function vtrack(name, extra) {
        SASA.track(name, Object.assign({
          video_id:    videoId,
          video_title: title,
          video_session: vsid,
          position:    Math.round(video.currentTime),
          duration:    Math.round(video.duration) || 0,
        }, extra || {}));
      }

      video.addEventListener('play',      function () { vtrack('video_play'); });
      video.addEventListener('pause',     function () { vtrack('video_pause'); });
      video.addEventListener('ended',     function () { vtrack('video_complete'); });
      video.addEventListener('seeking',   function () { vtrack('video_seek'); });
      video.addEventListener('error',     function () { vtrack('video_error'); });
      video.addEventListener('waiting',   function () { buffering = true;  vtrack('video_buffer_start'); });
      video.addEventListener('playing',   function () {
        if (buffering) { buffering = false; vtrack('video_buffer_end'); }
      });

      // heartbeat every 5 s while playing
      setInterval(function () {
        if (!video.paused && !video.ended) vtrack('video_heartbeat');
      }, 5000);

      log('attached video tracking to', videoId);
    }

    // attach to existing videos
    [].forEach.call(document.querySelectorAll('video'), attachVideo);

    // watch for dynamically added videos
    var mo = new MutationObserver(function (mutations) {
      mutations.forEach(function (m) {
        m.addedNodes.forEach(function (node) {
          if (node.tagName === 'VIDEO') attachVideo(node);
          if (node.querySelectorAll) {
            [].forEach.call(node.querySelectorAll('video'), attachVideo);
          }
        });
      });
    });
    mo.observe(document.body || document.documentElement, { childList: true, subtree: true });
  }

  // ── auto: JS error tracking ──────────────────────────────────────────────
  window.addEventListener('error', function (e) {
    SASA.track('js_error', {
      message: e.message,
      filename: e.filename,
      lineno: e.lineno,
      colno: e.colno,
    });
  });

  // ── fire initial page view ───────────────────────────────────────────────
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', trackPageView);
  } else {
    trackPageView();
  }

  // ── expose globally ──────────────────────────────────────────────────────
  global.SASA = SASA;
  log('SDK ready', CONFIG);

}(window));
