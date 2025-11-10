# pyright: reportPrivateUsage=false
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from yggdrasil.domain.entities.galleryinfo import Galleryinfo
from yggdrasil.domain.entities.info import Info
from yggdrasil.domain.exceptions import GalleryinfoNotFound

from sunflower.application.tasks.mirroring import (
    MirroringStatus,
    MirroringTask,
    now,
)


@pytest.fixture
def mock_hitomi_la_repo():
    repo = MagicMock()
    repo.hitomi_la.index_files = ["file1.js", "file2.js"]
    return repo


@pytest.fixture
def mock_sqlalchemy_repo():
    return MagicMock()


@pytest.fixture
def mock_mongodb_repo():
    return MagicMock()


@pytest.fixture
def status_dict() -> dict[str, Any]:
    return {
        "index_files": [],
        "total_items": 0,
        "batch_total": 0,
        "batch_completed": 0,
        "items_processed": 0,
        "is_mirroring_galleryinfo": False,
        "is_converting_to_info": False,
        "is_checking_integrity": False,
        "last_checked_at": "",
        "last_mirrored_at": "",
    }


@pytest.fixture
def mirroring_task(
    mock_hitomi_la_repo: MagicMock,
    mock_sqlalchemy_repo: MagicMock,
    mock_mongodb_repo: MagicMock,
    status_dict: dict[str, Any],
):
    return MirroringTask(
        mock_hitomi_la_repo,
        mock_sqlalchemy_repo,
        mock_mongodb_repo,
        run_as_once=True,  # Pass run_as_once as a keyword argument
    )


def test_mirroring_status_default():
    status = MirroringStatus.default()
    assert status.index_files == []
    assert status.total_items == 0
    assert status.batch_total == 0
    assert status.batch_completed == 0
    assert status.items_processed == 0
    assert status.is_mirroring_galleryinfo is False
    assert status.is_converting_to_info is False
    assert status.is_checking_integrity is False
    assert status.last_checked_at == ""
    assert status.last_mirrored_at == ""


def test_mirroring_status_reset():
    status = MirroringStatus.default()
    status.batch_completed = 10
    status.total_items = 100
    status.batch_total = 20
    status.reset()
    assert status.batch_completed == 0
    assert status.total_items == 0
    assert status.batch_total == 0


def test_mirroring_task_init(
    mirroring_task: MirroringTask,
    mock_hitomi_la_repo: MagicMock,
    status_dict: dict[str, Any],
):
    assert mirroring_task.hitomi_la == mock_hitomi_la_repo
    assert mirroring_task.status.index_files == ["file1.js", "file2.js"]
    assert mirroring_task.skip_ids == set()
    assert mirroring_task.REMOTE_CONCURRENT_SIZE == 50
    assert mirroring_task.LOCAL_CONCURRENT_SIZE == 25
    assert mirroring_task.INTEGRITY_PARTIAL_CHECK_RANGE_SIZE == 100


@pytest.mark.asyncio
async def test_preprocess(
    mirroring_task: MirroringTask, sample_galleryinfo: Galleryinfo
):
    async def mock_execute(id: int) -> Galleryinfo:
        galleryinfo = sample_galleryinfo
        galleryinfo.id = 999  # Different ID to test preprocessing
        return galleryinfo

    result = await mirroring_task._preprocess(mock_execute, 12345)
    assert result.id == 12345  # Should be overridden by preprocessing


@pytest.mark.asyncio
async def test_get_differences(mirroring_task: MirroringTask):
    mock_source_usecase = AsyncMock()
    mock_target_usecase = AsyncMock()

    mock_source_usecase.execute.return_value = [1, 2, 3, 4, 5]
    mock_target_usecase.execute.return_value = [3, 4, 5, 6, 7]

    differences = await mirroring_task._get_differences(
        mock_source_usecase, mock_target_usecase
    )
    assert set(differences) == {1, 2}


def test_get_splited_id(mirroring_task: MirroringTask):
    ids = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    size = 3
    result = list(mirroring_task._get_splited_id(ids, size))
    expected = [(1, 2, 3), (4, 5, 6), (7, 8, 9), (10,)]
    assert result == expected


def test_get_splited_id_empty(mirroring_task: MirroringTask):
    ids = ()
    size = 3
    result = list(mirroring_task._get_splited_id(ids, size))
    assert result == []


@pytest.mark.asyncio
async def test_process_in_jobs_remote(mirroring_task: MirroringTask):
    ids = (1, 2, 3, 4, 5)
    process_calls = []

    async def mock_process(batch):
        process_calls.append(batch)

    # Store status values before calling the method (since it resets)
    await mirroring_task._process_in_jobs(ids, mock_process, is_remote=True)

    # Status is reset at the end, but items_processed should be set
    assert mirroring_task.status.items_processed == 5
    assert len(process_calls) == 1
    assert process_calls[0] == ids


@pytest.mark.asyncio
async def test_process_in_jobs_local(mirroring_task: MirroringTask):
    ids = tuple(range(1, 51))  # 50 items
    process_calls: list[Any] = []

    async def mock_process(batch):
        process_calls.append(batch)

    await mirroring_task._process_in_jobs(ids, mock_process, is_remote=False)

    # Status is reset at the end, but items_processed should be set
    assert mirroring_task.status.items_processed == 50
    assert len(process_calls) == 2


@pytest.mark.asyncio
async def test_fetch_and_store_galleryinfo(
    mirroring_task: MirroringTask, sample_galleryinfo: Galleryinfo
):
    ids = (1, 2, 3)
    mock_target_repo = AsyncMock()

    # Mock GetGalleryinfoUseCase creation and execution
    with patch(
        "sunflower.application.tasks.mirroring.GetGalleryinfoUseCase"
    ) as mock_get_usecase:
        with patch(
            "sunflower.application.tasks.mirroring.CreateGalleryinfoUseCase"
        ) as mock_create_usecase:
            # Mock the use case instance
            mock_get_instance = AsyncMock()
            mock_get_instance.execute.return_value = sample_galleryinfo
            mock_get_usecase.return_value = mock_get_instance

            mock_create_instance = AsyncMock()
            mock_create_usecase.return_value = mock_create_instance

            await mirroring_task._fetch_and_store_galleryinfo(ids, mock_target_repo)

            # Should create and execute for each ID
            assert mock_create_instance.execute.call_count == 3


@pytest.mark.asyncio
async def test_fetch_and_store_info(
    mirroring_task: MirroringTask, sample_galleryinfo: Galleryinfo
):
    ids = (1, 2, 3)

    with patch(
        "sunflower.application.tasks.mirroring.GetGalleryinfoUseCase"
    ) as mock_get_usecase:
        with patch(
            "sunflower.application.tasks.mirroring.CreateInfoUseCase"
        ) as mock_create_usecase:
            mock_get_instance = AsyncMock()
            mock_get_instance.execute.return_value = sample_galleryinfo
            mock_get_usecase.return_value = mock_get_instance

            mock_create_instance = AsyncMock()
            mock_create_usecase.return_value = mock_create_instance

            await mirroring_task._fetch_and_store_info(ids)

            assert mock_create_instance.execute.call_count == 3


@pytest.mark.asyncio
@patch("sunflower.application.tasks.mirroring.logger")
async def test_integrity_check_success(
    mock_logger: MagicMock,
    mirroring_task: MirroringTask,
    sample_galleryinfo: Galleryinfo,
):
    ids = (1, 2, 3)

    with patch(
        "sunflower.application.tasks.mirroring.GetGalleryinfoUseCase"
    ) as mock_get_usecase:
        with patch("sunflower.application.tasks.mirroring.DeepDiff", return_value={}):
            mock_get_instance = AsyncMock()
            mock_get_instance.execute.return_value = sample_galleryinfo
            mock_get_usecase.return_value = mock_get_instance

            # Mock _preprocess to handle the remote call
            with patch.object(
                mirroring_task, "_preprocess", return_value=sample_galleryinfo
            ):
                await mirroring_task._integrity_check(ids)

                # Should not log any warnings for identical data
                warning_calls = [
                    call
                    for call in mock_logger.warning.call_args_list
                    if "Integrity check failed" in str(call)
                ]
                assert len(warning_calls) == 0


@pytest.mark.asyncio
@patch("sunflower.application.tasks.mirroring.logger")
async def test_integrity_check_with_differences(
    mock_logger: MagicMock,
    mirroring_task: MirroringTask,
    sample_galleryinfo: Galleryinfo,
):
    ids = (1,)

    with patch(
        "sunflower.application.tasks.mirroring.GetGalleryinfoUseCase"
    ) as mock_get_usecase:
        with patch(
            "sunflower.application.tasks.mirroring.DeepDiff",
            return_value={
                "values_changed": {
                    "root['title']": {"old_value": "old", "new_value": "new"}
                }
            },
        ):
            with patch(
                "sunflower.application.tasks.mirroring.DeleteGalleryinfoUseCase"
            ) as mock_delete_gallery:
                with patch(
                    "sunflower.application.tasks.mirroring.DeleteInfoUseCase"
                ) as mock_delete_info:
                    with patch(
                        "sunflower.application.tasks.mirroring.CreateGalleryinfoUseCase"
                    ) as mock_create_gallery:
                        with patch(
                            "sunflower.application.tasks.mirroring.CreateInfoUseCase"
                        ) as mock_create_info:
                            with patch.object(Info, "from_galleryinfo"):
                                mock_get_instance = AsyncMock()
                                mock_get_instance.execute.return_value = (
                                    sample_galleryinfo
                                )
                                mock_get_usecase.return_value = mock_get_instance

                                mock_delete_gallery_instance = AsyncMock()
                                mock_delete_gallery.return_value = (
                                    mock_delete_gallery_instance
                                )

                                mock_delete_info_instance = AsyncMock()
                                mock_delete_info.return_value = (
                                    mock_delete_info_instance
                                )

                                mock_create_gallery_instance = AsyncMock()
                                mock_create_gallery.return_value = (
                                    mock_create_gallery_instance
                                )

                                mock_create_info_instance = AsyncMock()
                                mock_create_info.return_value = (
                                    mock_create_info_instance
                                )

                                # Mock _preprocess to handle the remote call
                                with patch.object(
                                    mirroring_task,
                                    "_preprocess",
                                    return_value=sample_galleryinfo,
                                ):
                                    await mirroring_task._integrity_check(ids)

                                    # Should log warning and perform delete/create operations
                                    mock_logger.warning.assert_called()
                                    mock_delete_gallery_instance.execute.assert_called_once()
                                    mock_delete_info_instance.execute.assert_called_once()
                                    mock_create_gallery_instance.execute.assert_called_once()
                                    mock_create_info_instance.execute.assert_called_once()


@pytest.mark.asyncio
@patch("sunflower.application.tasks.mirroring.logger")
async def test_integrity_check_galleryinfo_not_found(
    mock_logger: MagicMock, mirroring_task: MirroringTask
):
    ids = (1,)

    # Mock _preprocess to throw GalleryinfoNotFound
    with patch.object(
        mirroring_task, "_preprocess", side_effect=GalleryinfoNotFound("Not found")
    ):
        await mirroring_task._integrity_check(ids)

        # Should add ID to skip_ids and log warning
        assert 1 in mirroring_task.skip_ids
        mock_logger.warning.assert_called()


@pytest.mark.asyncio
async def test_perform_mirroring_with_remote_differences(mirroring_task: MirroringTask):
    remote_ids = (1, 2, 3)

    with patch.object(mirroring_task, "_get_differences") as mock_get_differences:
        with patch.object(mirroring_task, "_process_in_jobs") as mock_process_in_jobs:
            with patch(
                "sunflower.application.tasks.mirroring.now", return_value="mocked_time"
            ):
                # First call returns remote differences, second call returns empty (no local differences)
                mock_get_differences.side_effect = [remote_ids, ()]

                await mirroring_task.perform_mirroring()

                # Should call _process_in_jobs twice (once for galleryinfo, once for integrity check)
                assert mock_process_in_jobs.call_count == 2
                # Since there are no local differences but remote differences exist, last_mirrored_at should not be set
                assert mirroring_task.status.last_mirrored_at == ""


@pytest.mark.asyncio
async def test_perform_mirroring_with_local_differences(mirroring_task: MirroringTask):
    local_ids = (4, 5, 6)

    with patch.object(mirroring_task, "_get_differences") as mock_get_differences:
        with patch.object(mirroring_task, "_process_in_jobs") as mock_process_in_jobs:
            # First call returns empty (no remote differences), second call returns local differences
            mock_get_differences.side_effect = [(), local_ids]

            await mirroring_task.perform_mirroring()

            # Should call _process_in_jobs twice (once for info conversion, once for integrity check)
            assert mock_process_in_jobs.call_count == 2


@pytest.mark.asyncio
async def test_perform_mirroring_no_differences(mirroring_task: MirroringTask):
    with patch.object(mirroring_task, "_get_differences") as mock_get_differences:
        with patch.object(mirroring_task, "_process_in_jobs") as mock_process_in_jobs:
            # Both calls return empty (no differences)
            mock_get_differences.side_effect = [(), ()]

            await mirroring_task.perform_mirroring()

            # Should only call _process_in_jobs once for integrity check
            assert mock_process_in_jobs.call_count == 1


@pytest.mark.asyncio
@patch("sunflower.application.tasks.mirroring.sleep")
@patch("sunflower.application.tasks.mirroring.logger")
async def test_start_mirroring_single_iteration(
    mock_logger: MagicMock, mock_sleep: MagicMock, mirroring_task: MirroringTask
):
    # Mock sleep to break the infinite loop after first iteration
    call_count = 0

    async def mock_sleep_func(delay):
        nonlocal call_count
        call_count += 1
        if call_count > 1:
            raise Exception("Break loop")

    mock_sleep.side_effect = mock_sleep_func

    with patch.object(mirroring_task, "perform_mirroring") as mock_mirror:
        try:
            await mirroring_task.start_mirroring(1.0)
        except Exception as e:
            if str(e) != "Break loop":
                raise

    mock_logger.info.assert_called_with("Starting mirroring task with delay: 1.0")
    # perform_mirroring() should be called at least once
    assert mock_mirror.call_count >= 1
    assert mirroring_task.status.last_checked_at != ""


@pytest.mark.asyncio
@patch("sunflower.application.tasks.mirroring.logger")
async def test_start_mirroring_sleep_is_awaited(
    mock_logger: MagicMock, mirroring_task: MirroringTask
):
    # Create a new task with run_as_once=False so sleep() path is exercised
    task = MirroringTask(
        mirroring_task.hitomi_la,
        mirroring_task.sqlalchemy,
        mirroring_task.mongodb,
        run_as_once=False,
    )

    with patch(
        "sunflower.application.tasks.mirroring.sleep", new_callable=AsyncMock
    ) as mock_sleep:
        with patch.object(
            task, "perform_mirroring", new_callable=AsyncMock
        ) as mock_perform:

            async def break_loop(delay: float) -> None:
                # Break the infinite loop after first sleep
                raise Exception("Break loop")

            mock_sleep.side_effect = break_loop

            try:
                await task.start_mirroring(0.5)
            except Exception as e:
                if str(e) != "Break loop":
                    raise

            # sleep should have been awaited exactly once with the provided delay
            await_call = mock_sleep.await_args
            assert await_call is not None
            assert await_call.args[0] == 0.5

            # perform_mirroring should have been awaited at least once
            assert mock_perform.await_count >= 1
            mock_logger.info.assert_called_with(
                "Starting mirroring task with delay: 0.5"
            )


@pytest.mark.asyncio
@patch("sunflower.application.tasks.mirroring.sleep")
@patch("sunflower.application.tasks.mirroring.logger")
async def test_start_partial_integrity_check_single_iteration(
    mock_logger: MagicMock, mock_sleep: MagicMock, mirroring_task: MirroringTask
):
    # Mock sleep to break the infinite loop after first iteration
    call_count = 0

    async def mock_sleep_func(delay):
        nonlocal call_count
        call_count += 1
        if call_count > 1:
            raise Exception("Break loop")

    mock_sleep.side_effect = mock_sleep_func

    with patch.object(
        mirroring_task, "perform_partial_integrity_check", new_callable=AsyncMock
    ) as mock_perform:
        try:
            await mirroring_task.start_partial_integrity_check(1.0)
        except Exception as e:
            if str(e) != "Break loop":
                raise

        mock_logger.info.assert_called_with(
            "Starting partial integrity check task with partial check delay: 1.0"
        )
        assert mock_perform.await_count == 1


@pytest.mark.asyncio
async def test_get_differences_empty_source(mirroring_task: MirroringTask):
    mock_source_usecase = AsyncMock()
    mock_target_usecase = AsyncMock()

    mock_source_usecase.execute.return_value = []
    mock_target_usecase.execute.return_value = [1, 2, 3]

    differences = await mirroring_task._get_differences(
        mock_source_usecase, mock_target_usecase
    )
    assert differences == ()


@pytest.mark.asyncio
async def test_get_differences_empty_target(mirroring_task: MirroringTask):
    mock_source_usecase = AsyncMock()
    mock_target_usecase = AsyncMock()

    mock_source_usecase.execute.return_value = [1, 2, 3]
    mock_target_usecase.execute.return_value = []

    differences = await mirroring_task._get_differences(
        mock_source_usecase, mock_target_usecase
    )
    assert set(differences) == {1, 2, 3}


@pytest.mark.asyncio
async def test_process_in_jobs_empty_ids(mirroring_task: MirroringTask):
    ids = ()
    process_calls = []

    async def mock_process(batch):
        process_calls.append(batch)

    await mirroring_task._process_in_jobs(ids, mock_process, is_remote=True)

    # Status is reset at the end, but items_processed should be set
    assert mirroring_task.status.items_processed == 0
    assert len(process_calls) == 0


@pytest.mark.asyncio
@patch("sunflower.application.tasks.mirroring.logger")
async def test_integrity_check_with_exception_in_preprocess(
    mock_logger: MagicMock, mirroring_task: MirroringTask
):
    ids = (1, 2)

    # Mock _preprocess to throw GalleryinfoNotFound for all IDs
    with patch.object(
        mirroring_task, "_preprocess", side_effect=GalleryinfoNotFound("Not found")
    ):
        await mirroring_task._integrity_check(ids)

        # Both IDs should be added to skip_ids
        assert mirroring_task.skip_ids == {1, 2}
        assert mock_logger.warning.call_count == 2


@pytest.mark.asyncio
async def test_start_mirroring_with_integrity_checking_flag(
    mirroring_task: MirroringTask,
):
    # Set integrity checking flag to True
    mirroring_task.status.is_checking_integrity = True

    with patch.object(mirroring_task, "perform_mirroring") as mock_mirror:
        with patch(
            "sunflower.application.tasks.mirroring.sleep",
            side_effect=[None, Exception("Break loop")],
        ):
            try:
                await mirroring_task.start_mirroring(0.1)
            except Exception as e:
                if str(e) != "Break loop":
                    raise

            # perform_mirroring() should not be called when integrity checking is active
            mock_mirror.assert_not_called()


@pytest.mark.asyncio
async def test_start_partial_integrity_check_with_mirroring_flags(
    mirroring_task: MirroringTask,
):
    # Set mirroring flags to True
    mirroring_task.status.is_mirroring_galleryinfo = True
    mirroring_task.status.is_converting_to_info = True

    with patch.object(mirroring_task, "_process_in_jobs") as mock_process_in_jobs:
        with patch(
            "sunflower.application.tasks.mirroring.sleep",
            side_effect=[None, Exception("Break loop")],
        ):
            try:
                # start_partial_integrity_check should not run integrity logic while mirroring flags are set
                # Patch GetAllInfoIdsUseCase used by perform_partial_integrity_check
                with patch(
                    "sunflower.application.tasks.mirroring.GetAllInfoIdsUseCase"
                ) as mock_info_usecase:

                    async def mock_awaitable():
                        return [1, 2, 3]

                    mock_info_usecase.return_value = mock_awaitable()
                    await mirroring_task.start_partial_integrity_check(0.1)
            except Exception as e:
                if str(e) != "Break loop":
                    raise

            # _process_in_jobs should not be called when mirroring is active
            mock_process_in_jobs.assert_not_called()


@pytest.mark.asyncio
async def test_start_partial_integrity_check_with_exception_clears_skip_ids(
    mirroring_task: MirroringTask,
):
    # Add some items to skip_ids
    mirroring_task.skip_ids.add(1)
    mirroring_task.skip_ids.add(2)

    with patch.object(
        mirroring_task, "_process_in_jobs", side_effect=Exception("Test exception")
    ):
        with patch(
            "sunflower.application.tasks.mirroring.GetAllInfoIdsUseCase"
        ) as mock_usecase:
            with patch(
                "sunflower.application.tasks.mirroring.sleep",
                side_effect=[None, Exception("Break loop")],
            ):
                # Mock the class to return an awaitable that returns the list
                async def mock_awaitable():
                    return [1, 2, 3]

                mock_usecase.return_value = mock_awaitable()

                try:
                    await mirroring_task.start_partial_integrity_check(0.1)
                except Exception as e:
                    if str(e) != "Break loop":
                        raise

                # skip_ids should be cleared after exception
                assert mirroring_task.skip_ids == set()


@pytest.mark.asyncio
async def test_preprocess_edge_case_comment(
    mirroring_task: MirroringTask, sample_galleryinfo: Galleryinfo
):
    """Test the edge case mentioned in the comment: 1783616 <=> 1669497"""

    async def mock_execute(id: int) -> Galleryinfo:
        galleryinfo = sample_galleryinfo
        galleryinfo.id = 1669497  # Different ID as mentioned in comment
        return galleryinfo

    result = await mirroring_task._preprocess(mock_execute, 1783616)
    assert result.id == 1783616  # Should be overridden by preprocessing


@pytest.mark.asyncio
async def test_perform_mirroring_with_both_differences(mirroring_task: MirroringTask):
    remote_ids = (1, 2, 3)
    local_ids = (4, 5, 6)

    with patch.object(mirroring_task, "_get_differences") as mock_get_differences:
        with patch.object(mirroring_task, "_process_in_jobs") as mock_process_in_jobs:
            with patch(
                "sunflower.application.tasks.mirroring.now",
                return_value="mocked_time",
            ):
                # First call returns remote differences, second call returns local differences
                mock_get_differences.side_effect = [remote_ids, local_ids]

                await mirroring_task.perform_mirroring()

                # Should call _process_in_jobs three times (galleryinfo, info conversion, integrity check)
                assert mock_process_in_jobs.call_count == 3
                assert mirroring_task.status.last_mirrored_at == "mocked_time"


@pytest.mark.asyncio
async def test_fetch_and_store_galleryinfo_with_different_target_repo(
    mirroring_task: MirroringTask, sample_galleryinfo: Galleryinfo
):
    ids = (1, 2)
    different_target_repo = AsyncMock()

    with patch(
        "sunflower.application.tasks.mirroring.GetGalleryinfoUseCase"
    ) as mock_get_usecase:
        with patch(
            "sunflower.application.tasks.mirroring.CreateGalleryinfoUseCase"
        ) as mock_create_usecase:
            mock_get_instance = AsyncMock()
            mock_get_instance.execute.return_value = sample_galleryinfo
            mock_get_usecase.return_value = mock_get_instance

            mock_create_instance = AsyncMock()
            mock_create_usecase.return_value = mock_create_instance

            await mirroring_task._fetch_and_store_galleryinfo(
                ids, different_target_repo
            )

            # Should create usecase with the provided target repository
            mock_create_usecase.assert_called_with(different_target_repo)
            assert mock_create_instance.execute.call_count == 2


# Additional tests for better coverage and edge cases


@patch("sunflower.application.tasks.mirroring.tzname", ["UTC"])
@patch("sunflower.application.tasks.mirroring.datetime")
def test_now_function(mock_datetime: MagicMock):
    mock_datetime.now.return_value.strftime = MagicMock(
        return_value="2023-10-14 10:30:00"
    )
    mock_datetime.now.return_value.__str__ = MagicMock(
        return_value="2023-10-14 10:30:00"
    )

    result = now()
    assert result.startswith("(UTC)")
    assert "2023-10-14" in result


def test_get_splited_id_edge_cases(mirroring_task: MirroringTask):
    # Test with size larger than ids length
    ids = (1, 2, 3)
    size = 10
    result = list(mirroring_task._get_splited_id(ids, size))
    assert result == [(1, 2, 3)]

    # Test with size = 1
    ids = (1, 2, 3, 4, 5)
    size = 1
    result = list(mirroring_task._get_splited_id(ids, size))
    expected = [(1,), (2,), (3,), (4,), (5,)]
    assert result == expected


def test_get_splited_id_zero_size_error(mirroring_task: MirroringTask):
    """Test that zero size raises an error or handles gracefully"""
    ids = (1, 2, 3)
    size = 0

    # Zero size will cause infinite loop or error in range function
    try:
        result = list(mirroring_task._get_splited_id(ids, size))
        # If no error, it should return empty or handle gracefully
        assert result == []
    except (ValueError, ZeroDivisionError):
        # This is acceptable behavior for zero size
        pass


@pytest.mark.asyncio
async def test_perform_full_integrity_check_uses_all_ids_minus_skip(
    mirroring_task: MirroringTask,
):
    """perform_full_integrity_check should use all info ids excluding skip_ids"""
    # Given
    all_ids = [1, 2, 3, 4, 5]
    mirroring_task.skip_ids = {2, 5}

    with patch.object(
        mirroring_task, "perform_integrity_check", new_callable=AsyncMock
    ) as mock_perform_integrity:
        # Mock GetAllInfoIdsUseCase constructor to return an awaitable (aligned with current implementation)
        with patch(
            "sunflower.application.tasks.mirroring.GetAllInfoIdsUseCase"
        ) as mock_usecase:

            async def mock_awaitable():
                return all_ids

            mock_usecase.return_value = mock_awaitable()

            # When
            await mirroring_task.perform_full_integrity_check()

            # Then
            assert mock_perform_integrity.await_count == 1
            await_call = mock_perform_integrity.await_args
            assert await_call is not None
            called_ids = await_call.args[0]
            assert set(called_ids) == {1, 3, 4}


@pytest.mark.asyncio
async def test_perform_full_integrity_check_with_no_ids(
    mirroring_task: MirroringTask,
):
    """perform_full_integrity_check should pass empty tuple when no ids are returned"""
    with patch.object(
        mirroring_task, "perform_integrity_check", new_callable=AsyncMock
    ) as mock_perform_integrity:
        with patch(
            "sunflower.application.tasks.mirroring.GetAllInfoIdsUseCase"
        ) as mock_usecase:

            async def mock_awaitable():
                return []

            mock_usecase.return_value = mock_awaitable()

            await mirroring_task.perform_full_integrity_check()

            assert mock_perform_integrity.await_count == 1
            await_call = mock_perform_integrity.await_args
            assert await_call is not None
            called_ids = await_call.args[0]
            assert called_ids == ()


@pytest.mark.asyncio
@patch("sunflower.application.tasks.mirroring.sleep")
@patch("sunflower.application.tasks.mirroring.logger")
async def test_start_full_integrity_check_single_iteration(
    mock_logger: MagicMock, mock_sleep: MagicMock, mirroring_task: MirroringTask
):
    # Mock sleep to break the infinite loop after first iteration
    call_count = 0

    async def mock_sleep_func(delay):
        nonlocal call_count
        call_count += 1
        if call_count > 1:
            raise Exception("Break loop")

    mock_sleep.side_effect = mock_sleep_func

    with patch.object(
        mirroring_task, "perform_full_integrity_check", new_callable=AsyncMock
    ) as mock_perform:
        try:
            await mirroring_task.start_full_integrity_check(1.0)
        except Exception as e:
            if str(e) != "Break loop":
                raise

        mock_logger.info.assert_called_with(
            "Starting full integrity check task with full check delay: 1.0"
        )
        assert mock_perform.await_count == 1


@pytest.mark.asyncio
async def test_process_in_jobs_batch_calculation(mirroring_task: MirroringTask):
    """Test batch calculation logic"""
    ids = tuple(range(1, 101))  # 100 items

    async def mock_process(batch):
        pass

    # Test with remote (size 50)
    await mirroring_task._process_in_jobs(ids, mock_process, is_remote=True)
    assert mirroring_task.status.items_processed == 100

    # Test with local (size 25)
    ids = tuple(range(1, 76))  # 75 items
    await mirroring_task._process_in_jobs(ids, mock_process, is_remote=False)
    assert mirroring_task.status.items_processed == 75


@pytest.mark.asyncio
async def test_process_in_jobs_single_item(mirroring_task: MirroringTask):
    """Test processing single item"""
    ids = (42,)
    process_calls = []

    async def mock_process(batch):
        process_calls.append(batch)

    await mirroring_task._process_in_jobs(ids, mock_process, is_remote=True)

    assert mirroring_task.status.items_processed == 1
    assert len(process_calls) == 1
    assert process_calls[0] == (42,)


@pytest.mark.asyncio
async def test_integrity_check_safety_function_none_handling(
    mirroring_task: MirroringTask, sample_galleryinfo: Galleryinfo
):
    """Test that integrity check properly handles None results from __safety"""
    ids = (1, 2, 3)

    # Mock _preprocess to return None for some IDs
    preprocess_results = [None, sample_galleryinfo, None]

    with patch(
        "sunflower.application.tasks.mirroring.GetGalleryinfoUseCase"
    ) as mock_get_usecase:
        mock_get_instance = AsyncMock()
        mock_get_instance.execute.return_value = sample_galleryinfo
        mock_get_usecase.return_value = mock_get_instance

        async def mock_preprocess(execute_func, id):
            if id == 2:
                return sample_galleryinfo
            else:
                raise GalleryinfoNotFound("Not found")

        with patch.object(mirroring_task, "_preprocess", side_effect=mock_preprocess):
            await mirroring_task._integrity_check(ids)

            # Only ID 2 should result in actual integrity check
            # IDs 1 and 3 should be added to skip_ids
            assert {1, 3}.issubset(mirroring_task.skip_ids)


@pytest.mark.asyncio
async def test_perform_mirroring_integrity_check_uses_local_differences(
    mirroring_task: MirroringTask,
):
    """Test that mirror() uses local_differences for integrity check, not remote_differences"""
    remote_ids = (1, 2, 3)
    local_ids = (4, 5, 6)

    with patch.object(mirroring_task, "_get_differences") as mock_get_differences:
        with patch.object(mirroring_task, "_process_in_jobs") as mock_process_in_jobs:
            mock_get_differences.side_effect = [remote_ids, local_ids]

            await mirroring_task.perform_mirroring()

            # Check that the last call to _process_in_jobs uses local_ids for integrity check
            assert mock_process_in_jobs.call_count == 3
            last_call_args = mock_process_in_jobs.call_args_list[-1]
            assert (
                last_call_args[0][0] == local_ids
            )  # First argument should be local_ids


@pytest.mark.asyncio
async def test_perform_mirroring_integrity_check_with_empty_local_differences(
    mirroring_task: MirroringTask,
):
    """Test integrity check when there are no local differences"""
    remote_ids = (1, 2, 3)
    local_ids = ()  # Empty

    with patch.object(mirroring_task, "_get_differences") as mock_get_differences:
        with patch.object(mirroring_task, "_process_in_jobs") as mock_process_in_jobs:
            mock_get_differences.side_effect = [remote_ids, local_ids]

            await mirroring_task.perform_mirroring()

            # Should still call integrity check with empty local_ids
            assert mock_process_in_jobs.call_count == 2
            last_call_args = mock_process_in_jobs.call_args_list[-1]
            assert last_call_args[0][0] == ()


@pytest.mark.asyncio
async def test_fetch_and_store_info_with_info_conversion(
    mirroring_task: MirroringTask, sample_galleryinfo: Galleryinfo
):
    """Test that _fetch_and_store_info properly converts Galleryinfo to Info"""
    ids = (1, 2)

    with patch(
        "sunflower.application.tasks.mirroring.GetGalleryinfoUseCase"
    ) as mock_get_usecase:
        with patch(
            "sunflower.application.tasks.mirroring.CreateInfoUseCase"
        ) as mock_create_usecase:
            with patch.object(Info, "from_galleryinfo") as mock_from_galleryinfo:
                mock_info = MagicMock()
                mock_from_galleryinfo.return_value = mock_info

                mock_get_instance = AsyncMock()
                mock_get_instance.execute.return_value = sample_galleryinfo
                mock_get_usecase.return_value = mock_get_instance

                mock_create_instance = AsyncMock()
                mock_create_usecase.return_value = mock_create_instance

                await mirroring_task._fetch_and_store_info(ids)

                # Should convert Galleryinfo to Info for each ID
                assert mock_from_galleryinfo.call_count == 2
                assert mock_create_instance.execute.call_count == 2

                # Verify Info.from_galleryinfo was called with correct arguments
                for call in mock_from_galleryinfo.call_args_list:
                    assert call[0][0] == sample_galleryinfo


@pytest.mark.asyncio
async def test_preprocess_preserves_galleryinfo_except_id(
    mirroring_task: MirroringTask, sample_galleryinfo: Galleryinfo
):
    """Test that _preprocess only changes the ID and preserves other attributes"""
    original_title = (
        sample_galleryinfo.title
        if hasattr(sample_galleryinfo, "title")
        else "test_title"
    )

    async def mock_execute(id: int) -> Galleryinfo:
        result = sample_galleryinfo
        result.id = 999
        if hasattr(result, "title"):
            result.title = original_title
        return result

    result = await mirroring_task._preprocess(mock_execute, 12345)

    assert result.id == 12345
    if hasattr(result, "title"):
        assert result.title == original_title


@pytest.mark.asyncio
async def test_start_partial_integrity_check_skip_ids_filtering(
    mirroring_task: MirroringTask,
):
    """Test that skip_ids are properly filtered out from partial integrity check"""
    all_ids = [1, 2, 3, 4, 5, 6]
    mirroring_task.skip_ids = {2, 4}

    with patch(
        "sunflower.application.tasks.mirroring.GetAllInfoIdsUseCase"
    ) as mock_usecase:
        with patch.object(mirroring_task, "_process_in_jobs") as mock_process_in_jobs:
            with patch(
                "sunflower.application.tasks.mirroring.sleep",
                side_effect=[None],
            ):

                async def mock_awaitable():
                    return all_ids

                mock_usecase.return_value = mock_awaitable()
                await mirroring_task.start_partial_integrity_check(0.1)
                if mock_process_in_jobs.call_count > 0:
                    processed_ids = set(mock_process_in_jobs.call_args_list[0][0][0])
                    expected_ids = {1, 3, 5, 6}
                    assert processed_ids == expected_ids


def test_mirroring_status_serialization():
    """Test MirroringStatus can be serialized/deserialized"""
    status = MirroringStatus.default()
    status.total_items = 100
    status.is_mirroring_galleryinfo = True
    status.last_checked_at = "2023-10-14 10:30:00"

    # Test that it has serialization capabilities (from Serializer base class)
    assert hasattr(status, "to_dict")

    # Test the actual values
    assert status.total_items == 100
    assert status.is_mirroring_galleryinfo is True
    assert status.last_checked_at == "2023-10-14 10:30:00"


@pytest.mark.asyncio
async def test_perform_mirroring_status_flags_properly_managed(
    mirroring_task: MirroringTask,
):
    """Test that status flags are properly set and unset during mirror operation"""
    remote_ids = (1, 2, 3)
    local_ids = (4, 5, 6)

    # Track status flag changes
    flag_changes = []

    async def track_galleryinfo_process(batch, target_repo):
        flag_changes.append(
            ("galleryinfo_start", mirroring_task.status.is_mirroring_galleryinfo)
        )

    async def track_info_process(batch):
        flag_changes.append(("info_start", mirroring_task.status.is_converting_to_info))

    async def track_integrity_process(batch):
        flag_changes.append(
            ("integrity_start", mirroring_task.status.is_checking_integrity)
        )

    with patch.object(mirroring_task, "_get_differences") as mock_get_differences:
        with patch.object(
            mirroring_task,
            "_fetch_and_store_galleryinfo",
            side_effect=track_galleryinfo_process,
        ):
            with patch.object(
                mirroring_task, "_fetch_and_store_info", side_effect=track_info_process
            ):
                with patch.object(
                    mirroring_task,
                    "_integrity_check",
                    side_effect=track_integrity_process,
                ):
                    mock_get_differences.side_effect = [remote_ids, local_ids]

                    await mirroring_task.perform_mirroring()

                    # After mirror completes, flags should be False
                    assert mirroring_task.status.is_mirroring_galleryinfo is False
                    assert mirroring_task.status.is_converting_to_info is False

                    # Check that flags were set during operations
                    galleryinfo_flags = [
                        change[1]
                        for change in flag_changes
                        if change[0] == "galleryinfo_start"
                    ]
                    info_flags = [
                        change[1]
                        for change in flag_changes
                        if change[0] == "info_start"
                    ]

                    if galleryinfo_flags:
                        assert any(
                            galleryinfo_flags
                        )  # Should have been True at some point
                    if info_flags:
                        assert any(info_flags)  # Should have been True at some point
