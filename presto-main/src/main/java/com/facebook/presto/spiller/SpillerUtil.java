/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spiller;

import com.facebook.presto.common.Page;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;

public class SpillerUtil
{
    public static class CompactPageIterator
            implements Iterator<Page>
    {
        private final Iterator<Page> delegate;

        CompactPageIterator(Iterator<Page> pageIterator)
        {
            this.delegate = requireNonNull(pageIterator, "pageIterator is null");
        }

        @Override
        public boolean hasNext()
        {
            return delegate.hasNext();
        }

        @Override
        public Page next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Page nextPage = delegate.next();
            nextPage.compact();
            return nextPage;
        }
    }
}
