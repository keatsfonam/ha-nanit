"""Camera platform for Nanit."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime

from homeassistant.components.camera import Camera, CameraEntityFeature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from aionanit import NanitCamera
from aionanit.camera import RequestType, StreamIdentifier, Streaming, StreamingStatus

from . import NanitConfigEntry
from .coordinator import NanitPushCoordinator
from .const import CONF_LOCAL_RTMP_PUBLISH_URLS, CONF_LOCAL_RTSP_STREAM_URLS
from .entity import NanitEntity

PARALLEL_UPDATES = 0

_LOGGER = logging.getLogger(__name__)

_STREAM_START_ATTEMPTS = 3
_STREAM_RETRY_DELAY = 2.0
_SNAPSHOT_CACHE_TTL = 60.0
_SNAPSHOT_PREFETCH_AGE = 30.0


async def async_setup_entry(
    hass: HomeAssistant,
    entry: NanitConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Nanit camera entities for all cameras on the account."""
    async_add_entities(
        NanitCameraEntity(cam_data.push_coordinator, cam_data.camera)
        for cam_data in entry.runtime_data.cameras.values()
    )


class NanitCameraEntity(NanitEntity, Camera):
    """Nanit camera entity — stream via RTMPS, snapshots from cloud."""

    _attr_translation_key = "camera"
    _attr_entity_registry_enabled_default = True
    _attr_supported_features = CameraEntityFeature.ON_OFF | CameraEntityFeature.STREAM

    def __init__(
        self,
        coordinator: NanitPushCoordinator,
        camera: NanitCamera,
    ) -> None:
        """Initialize."""
        super().__init__(coordinator)
        Camera.__init__(self)
        self._camera = camera
        self._prev_is_on: bool | None = None
        self._prev_last_seen: datetime | None = None
        self._attr_unique_id = f"{camera.uid}_camera"
        self._cached_snapshot: bytes | None = None
        self._cached_snapshot_at: float = 0.0

    @property
    def is_on(self) -> bool:
        """Return true if the camera is on (not in sleep/standby mode)."""
        if self.coordinator.data is None:
            return True
        sleep_mode = self.coordinator.data.settings.sleep_mode
        if sleep_mode is None:
            return True
        return not sleep_mode

    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        cur_on = self.is_on
        prev_on = self._prev_is_on
        self._prev_is_on = cur_on

        if prev_on is not None and prev_on != cur_on:
            # Camera power changed — invalidate cached stream.
            self._invalidate_stream("power state change")

        # Invalidate stream after WebSocket reconnection so the RTMPS URL
        # gets a fresh access token.  last_seen is updated only when the
        # transport moves to CONNECTED, so this fires once per reconnect.
        if self.coordinator.data is not None:
            cur_last_seen = self.coordinator.data.connection.last_seen
            if (
                self._prev_last_seen is not None
                and cur_last_seen != self._prev_last_seen
                and self.stream is not None
            ):
                self._invalidate_stream("WebSocket reconnection (token refreshed)")
            self._prev_last_seen = cur_last_seen

        super()._handle_coordinator_update()

    def _invalidate_stream(self, reason: str = "state change") -> None:
        """Discard HA's cached stream so a fresh one is created on next view."""
        if self.stream is not None:
            _LOGGER.debug("Invalidating cached stream after %s", reason)
            self.stream = None

    # ------------------------------------------------------------------
    # Streaming
    # ------------------------------------------------------------------

    async def stream_source(self) -> str | None:
        """Return the configured local relay URL or Nanit cloud RTMPS URL.

        When both local relay URLs are configured, this entity asks Nanit to
        publish RTMP into the local relay, then returns the relay RTSP URL for
        Home Assistant to consume.  Without local relay configuration, it falls
        back to the upstream Nanit cloud RTMPS path.
        """
        if not self.is_on:
            return None

        local_rtsp_stream_url = self._local_rtsp_stream_url
        if local_rtsp_stream_url:
            local_rtmp_publish_url = self._local_rtmp_publish_url
            if local_rtmp_publish_url and not await self._async_start_streaming_safe(
                local_rtmp_publish_url
            ):
                return None
            return local_rtsp_stream_url

        if not await self._async_start_streaming_safe():
            return None

        try:
            return await self._camera.async_get_stream_rtmps_url()
        except Exception:  # noqa: BLE001
            _LOGGER.warning("Failed to build RTMPS stream URL", exc_info=True)
            return None

    @property
    def _local_rtmp_publish_url(self) -> str | None:
        """Return configured local RTMP publish URL for this camera, if any."""
        return self._format_local_stream_url(CONF_LOCAL_RTMP_PUBLISH_URLS)

    @property
    def _local_rtsp_stream_url(self) -> str | None:
        """Return configured local RTSP stream URL for this camera, if any."""
        return self._format_local_stream_url(CONF_LOCAL_RTSP_STREAM_URLS)

    def _format_local_stream_url(self, option_key: str) -> str | None:
        """Format a per-camera local stream URL option."""
        urls = self.coordinator.config_entry.options.get(option_key, {})
        url = urls.get(self._camera.uid)
        if not url:
            return None
        return url.format(
            baby_uid=self.coordinator.baby.uid,
            camera_uid=self._camera.uid,
        )

    async def _async_start_streaming_safe(self, rtmp_url: str | None = None) -> bool:
        """Send PUT_STREAMING with retry.  Returns True on success."""
        for attempt in range(1, _STREAM_START_ATTEMPTS + 1):
            try:
                if rtmp_url is None:
                    await self._camera.async_start_streaming()
                else:
                    await self._async_start_streaming_to_url(rtmp_url)
                return True
            except Exception:  # noqa: BLE001
                if attempt < _STREAM_START_ATTEMPTS:
                    _LOGGER.debug(
                        "PUT_STREAMING attempt %d/%d failed for camera %s, retrying in %.0fs",
                        attempt,
                        _STREAM_START_ATTEMPTS,
                        self._camera.uid,
                        _STREAM_RETRY_DELAY,
                    )
                    await asyncio.sleep(_STREAM_RETRY_DELAY)
                else:
                    _LOGGER.warning(
                        "PUT_STREAMING failed after %d attempts for camera %s",
                        _STREAM_START_ATTEMPTS,
                        self._camera.uid,
                        exc_info=True,
                    )
        return False

    async def _async_start_streaming_to_url(self, rtmp_url: str) -> None:
        """Send PUT_STREAMING STARTED with an explicit RTMP publish URL."""
        streaming = Streaming(
            id=StreamIdentifier.MOBILE,
            status=StreamingStatus.STARTED,
            rtmp_url=rtmp_url,
        )
        await self._camera._send_request(  # noqa: SLF001
            RequestType.PUT_STREAMING,
            streaming=streaming,
        )

    # ------------------------------------------------------------------
    # Snapshot (with caching)
    # ------------------------------------------------------------------

    async def async_camera_image(
        self, width: int | None = None, height: int | None = None
    ) -> bytes | None:
        """Return a still image, using a cached snapshot when possible.

        Cache strategy:
        - Fresh cache (< TTL): return immediately.
        - Stale cache (> TTL): attempt a fresh fetch; return stale on failure.
        - No cache: fetch synchronously.

        A background prefetch is scheduled when the cache reaches
        ``_SNAPSHOT_PREFETCH_AGE`` so subsequent requests hit a warm cache.
        """
        if not self.is_on:
            return None

        now = time.monotonic()
        cache_age = now - self._cached_snapshot_at

        if self._cached_snapshot is not None and cache_age < _SNAPSHOT_CACHE_TTL:
            if cache_age >= _SNAPSHOT_PREFETCH_AGE:
                self.hass.async_create_background_task(
                    self._async_refresh_snapshot(),
                    name=f"nanit_snapshot_refresh_{self._camera.uid}",
                )
            return self._cached_snapshot

        fresh = await self._async_fetch_snapshot()
        if fresh is not None:
            return fresh

        return self._cached_snapshot

    async def _async_refresh_snapshot(self) -> None:
        """Background task: update the snapshot cache without blocking callers."""
        await self._async_fetch_snapshot()

    async def _async_fetch_snapshot(self) -> bytes | None:
        """Fetch a snapshot from the cloud and update the cache."""
        try:
            image = await self._camera.async_get_snapshot()
        except Exception:  # noqa: BLE001
            _LOGGER.debug("Failed to fetch snapshot for %s", self._camera.uid)
            return None
        if image is not None:
            self._cached_snapshot = image
            self._cached_snapshot_at = time.monotonic()
        return image

    # ------------------------------------------------------------------
    # On/off
    # ------------------------------------------------------------------

    async def async_turn_on(self) -> None:
        """Turn the camera on (disable sleep/standby mode)."""
        self._invalidate_stream()
        await self._camera.async_set_settings(sleep_mode=False)

    async def async_turn_off(self) -> None:
        """Turn the camera off (enable sleep/standby mode)."""
        self._invalidate_stream()
        try:
            await self._camera.async_stop_streaming()
        except Exception:  # noqa: BLE001
            _LOGGER.debug("Failed to stop streaming before sleep", exc_info=True)
        await self._camera.async_set_settings(sleep_mode=True)
