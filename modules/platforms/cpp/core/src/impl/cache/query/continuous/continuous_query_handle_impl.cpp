/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ignite/impl/cache/query/continuous/continuous_query_handle_impl.h"

using namespace ignite::common::concurrent;
using namespace ignite::jni::java;
using namespace ignite::java;
using namespace ignite::impl::interop;
using namespace ignite::impl::binary;

namespace ignite
{
    namespace impl
    {
        namespace cache
        {
            namespace query
            {
                namespace continuous
                {
                    struct Command
                    {
                        enum Type
                        {
                            GET_INITIAL_QUERY = 0,

                            CLOSE = 1
                        };
                    };

                    ContinuousQueryHandleImpl::ContinuousQueryHandleImpl(SP_IgniteEnvironment env, int64_t handle, jobject javaRef) :
                        env(env),
                        handle(handle),
                        javaRef(javaRef),
                        mutex(),
                        extracted(false)
                    {
                        // No-op.
                    }

                    ContinuousQueryHandleImpl::~ContinuousQueryHandleImpl()
                    {
                        env.Get()->Context()->TargetInLongOutLong(javaRef, Command::CLOSE, 0);

                        JniContext::Release(javaRef);

                        env.Get()->GetHandleRegistry().Release(handle);
                    }

                    QueryCursorImpl* ContinuousQueryHandleImpl::GetInitialQueryCursor(IgniteError& err)
                    {
                        CsLockGuard guard(mutex);

                        if (extracted)
                        {
                            err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                                "GetInitialQueryCursor() can be called only once.");

                            return 0;
                        }

                        JniErrorInfo jniErr;

                        jobject res = env.Get()->Context()->TargetOutObject(javaRef, Command::GET_INITIAL_QUERY, &jniErr);

                        IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                        if (jniErr.code != IGNITE_JNI_ERR_SUCCESS)
                            return 0;

                        extracted = true;

                        return new QueryCursorImpl(env, res);
                    }
                }
            }
        }
    }
}
